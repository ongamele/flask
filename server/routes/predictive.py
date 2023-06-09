import binascii
import couchdb
import pandas as pd
import numpy as np
import datetime as dt

from server import app, credentials, webapp
from flask import jsonify, request

import requests

from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor

from server.routes.autoai import AutoAI
from server.routes.practice import timezone_info


def connect_db():
    try:
        couch = couchdb.Server(credentials['custom_url'])  # establish DB connection
        x = couch.version()
        if isinstance(x, float):
            return couch
    except binascii.Error:
        status = {"status": 401, "message": "Could not connect."}
        return status

    return couch


def parameters(couch):
    """Get predictive model parameters

    Args:
        couch: Database connection object

    Returns: Best practice parameters
    """

    db = couch['model-parameters']
    return db['weight-prediction-model']


class MortalityPredictions:

    def __init__(self, broilerID, noChickens, start_date, end_date, current_date, n_days, cycle_id):

        self.broilerID = broilerID
        self.noChickens = noChickens
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = current_date
        self.n_days = n_days
        self.cycle_id = cycle_id

        self.mortalities = None
        self.current_mortalities = None

        self.m = None
        self.model_name = None
        self.cycles_len = 0

    def broiler_info(self):
        d = {
            'broiler_info': {
                'broiler_ID': str(self.broilerID),
                'start_date': self.start_date,
                'current_date': self.current_date,
                'end_date': self.end_date,
                'broiler_cycle_day': (
                        pd.to_datetime(self.current_date).date() - pd.to_datetime(self.start_date).date()).days,
                'cycle_id': str(self.cycle_id),
            }
        }
        return d

    def _load_historic_mort(self):

        db_url_yml = "https://f0d0d29c-7c5c-4212-9e05-6aa455b05adc-bluemix" \
                     ":e608b4ab60210ce0f9cf016e6d91efa45d515f3a57a2c2086b670f4cbdf30171@f0d0d29c-7c5c-4212-9e05-6aa455b05adc" \
                     "-bluemix.cloudantnosqldb.appdomain.cloud"
        couch = couchdb.Server(db_url_yml)
        the_cycles = {}
        if 'historical' not in couch:
            self.m = 'No historical data in all broiler houses'
        else:
            db = couch['historical']  # historical exists
            if str(self.broilerID) in db:  #
                doc = db[str(self.broilerID)]['file']  # whole excel file
                cycles = []
                for i in doc:
                    cycles.append(pd.DataFrame(doc[i]['doc']))  # cycles contains list of dataframes
                the_cycles[str(self.broilerID)] = cycles
                self.cycles_len = len(cycles)
                self.m = 'Local model was used'

                if len(the_cycles[str(self.broilerID)]) <= 10:  # not enough data, use global model

                    self.m = 'There is historical data, but defaulting to global model because there is not enough historical data'
                    docs = db.view("_all_docs", include_docs=True)
                    self.cycles_len = 0
                    for i in docs:
                        i = i['doc']
                        if i['_id'].startswith('_'):
                            continue
                        doc = i['file']
                        cycles1 = []
                        for j in doc:
                            cycles1.append(pd.DataFrame(doc[j]['doc']))
                        the_cycles[str(i['_id'])] = cycles1  # cycles1 is a list of dataframes
                        self.cycles_len += len(cycles1)

            else:  # use all data, ie global model

                self.m = 'broilerID does not have historical data, defaulting to global model'
                docs = db.view("_all_docs", include_docs=True)
                self.cycles_len = 0
                for i in docs:
                    i = i['doc']
                    if i['_id'].startswith('_'):
                        continue
                    doc = i['file']
                    cycles1 = []
                    for j in doc:
                        cycles1.append(pd.DataFrame(doc[j]['doc']))
                    the_cycles[str(i['_id'])] = cycles1  # cycles1 is a list of dataframes
                    self.cycles_len += len(cycles1)
        init_broilerID = list(the_cycles)[0]

        merged_df = the_cycles[init_broilerID][0][["AGE", "%CUMM"]]

        for broilerID_key in list(the_cycles.keys()):

            for cycle in range(len(the_cycles[broilerID_key])):

                if not (broilerID_key == init_broilerID and cycle == 0):
                    merged_df = pd.merge(merged_df, the_cycles[broilerID_key][cycle][["AGE", "%CUMM"]], on="AGE",
                                         how="outer")

        merged_df = merged_df.apply(lambda x: x.fillna(x.max()), axis=0)
        merged_df = merged_df.drop(['AGE'], axis=1)
        merged_df = merged_df.transpose()

        self.mortalities = merged_df.values

        return self

    def _get_current_mortalities_array(self):
        """
        Get current mortalities in the current broiler cycle
        """

        api_call = requests.get(f'{webapp}:178/getBriolerDetails/{self.broilerID}',
                                proxies={"http": None, "https": None, })

        mortality_data = []

        for i in api_call.json()['tbl_BriolerData']:
            mortality_data.append({"broilerID": i['briolerID'], "fatalities": i['fatalities'], "culls": i['culls'],
                                   "date": i['dateTime']})

        mortality_df = pd.DataFrame(mortality_data)

        mortality_df["date"] = mortality_df["date"].apply(lambda x: pd.to_datetime(x).date())
        mortality_df = mortality_df.sort_values(by=["date"])

        mortality_df = mortality_df[(mortality_df["date"] <= pd.to_datetime(self.current_date).date()) & (
                mortality_df["date"] >= pd.to_datetime(self.start_date).date())]
        mortality_df["AGE"] = mortality_df["date"].apply(
            lambda x: (pd.to_datetime(x).date() - pd.to_datetime(self.start_date).date()).days) + 1
        mortality_df["MORT"] = mortality_df["fatalities"] + mortality_df["culls"]

        # calculate mortality percent
        mortality_df["CUMM"] = mortality_df["MORT"].cumsum()
        mortality_df["BALANCE"] = self.noChickens - mortality_df["CUMM"] + mortality_df["MORT"]
        mortality_df["DAY%"] = (mortality_df["MORT"] / mortality_df["BALANCE"])  # *100
        mortality_df["%CUMM"] = mortality_df["DAY%"].cumsum()
        self.current_mortalities = np.array([mortality_df["%CUMM"].values])

        return self

    def fit_model(self, pred_data, n_days):

        current_age = (pd.to_datetime(self.current_date).date() - pd.to_datetime(
            self.start_date).date()).days + 1  # get current age of chickens
        max_age = (pd.to_datetime(self.end_date).date() - pd.to_datetime(
            self.start_date).date()).days + 1  # get max number of days

        if current_age <= max_age - n_days:  # Calculates how many days we need to predict if we are at the end of our cylce, eg if on day 33 and cycle ends at day 35
            # we are only require to predict last 2 days instead of n_days

            X = self.mortalities[:, 0:current_age]
            y = self.mortalities[:, current_age:current_age + n_days]

        else:  # if we are not at the end of the cycle then we predict n_days
            X = self.mortalities[:, 0:current_age]
            y = self.mortalities[:, current_age:current_age + (max_age - current_age)]

        if self.cycles_len >= 100:
            a = AutoAI()
            model = a.model_selection(X, y)
            model.fit(X, y)
        else:
            model = KNeighborsRegressor(8)  # LinearRegression(fit_intercept=True)
            model.fit(X, y)
        self.model_name = type(model).__name__
        predictions = model.predict(pred_data)  # pred_data is current day in the cycle

        return predictions

    def predict(self):
        self.start_date = str((pd.to_datetime(self.start_date) + dt.timedelta(days=1)).date())

        self._load_historic_mort()
        self._get_current_mortalities_array()

        try:
            predictions = self.fit_model(self.current_mortalities, self.n_days)
        except:
            cycle_name = requests.get(f'{webapp}:178/getCylces',
                                      proxies={"http": None, "https": None, }).json()
            cycle_name_df = pd.DataFrame(cycle_name)
            cycle_name_df = cycle_name_df.sort_values(by='id')
            cycle_name = cycle_name_df[cycle_name_df["broilerid"] == self.broilerID]["name"].values[-1]
            self.start_date = str((pd.to_datetime(self.start_date) - dt.timedelta(days=1)).date())
            return {"status": 400, "massage": "Please ensure mortality data is up to date from start date " +
                                              str(self.start_date) + " including current date " +
                                              str(self.current_date) + " for " + cycle_name +
                                              " and please only enter a single record for mortality and culls"}

        self.start_date = str((pd.to_datetime(self.start_date) - dt.timedelta(days=1)).date())

        pred_dict = {"predictions": {}}
        pred_dict["predictions"]["n_days"] = self.n_days

        if self.m:
            pred_dict["predictions"]['message'] = self.m

        if self.model_name:
            pred_dict["predictions"]['model_name'] = self.model_name

        current_age = (pd.to_datetime(self.current_date).date() - pd.to_datetime(self.start_date).date()).days

        for value, prediction in enumerate(predictions[0]):
            enum = value + 1
            pred_dict["predictions"][str((pd.to_datetime(self.current_date) + dt.timedelta(days=enum)).date())] = {}
            pred_dict["predictions"][str((pd.to_datetime(self.current_date) + dt.timedelta(days=enum)).date())][
                "broiler_cycle_day"] = current_age + enum

            pred_dict["predictions"][str((pd.to_datetime(self.current_date) + dt.timedelta(days=enum)).date())][
                "cumulative_percentage"] = np.absolute(prediction) * 100
            pred_dict["predictions"][str((pd.to_datetime(self.current_date) + dt.timedelta(days=enum)).date())][
                "cumulative_mortality"] = self.noChickens - int(self.noChickens * (1 - np.absolute(prediction)))

        return pred_dict


@app.route("/v1/api/model/predictive/mortality", methods=['POST'])
def predict_():
    """This API predict the mortality of a bird given the age.

        tags:
          - API
        parameters:
            - name: broilerID
              in: query
              type: string
              required: true
              description: The days to be predicted
            - name: cycle_age
              in: query
              type: string
              required: true
              description: The age of the cycle
        responses:
          200:
            description: Created
            schema:
              id: Created
              properties:
                mortality:
                  type: dict
                day:
                  type: string
                confidence:
                  type: float
                value:
                  type: float
          400:
            description: Bad Request
            schema:
              id: Bad Request
              properties:
                status:
                  type: integer
                  default: 400
                message:
                  type: string
          412:
            description: Precondition Failed
            schema:
              id: Precondition Failed
              properties:
                status:
                  type: integer
                  default: 412
                message:
                  type: string
        """
    data = request.form.to_dict()

    broilerID = data.get('broilerID')
    noChickens = data.get('noChickens')
    n_days = data.get('n_days')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    current_date = data.get('current_date')
    cycle_id = data.get('cycle_id')
    timezone = data.get('timezone', 2)

    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return status
    if not noChickens:
        status = {"status": 400, "message": "noChickens missing"}
        return status
    noChickens = int(noChickens)
    if not n_days:
        status = {"status": 400, "message": "n_days missing"}
        return status
    n_days = int(n_days)
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return status
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return status
    if not current_date:
        status = {"status": 400, "message": "current_date missing"}
        return status
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return status
    if not timezone:
        status = {"status": 400, "message": "timezone missing"}
        return status

    mort_model = MortalityPredictions(broilerID=int(broilerID), noChickens=noChickens, start_date=start_date,
                                      end_date=end_date, current_date=current_date, n_days=n_days, cycle_id=cycle_id)
    try:
        k = mort_model.predict()
        k.update(mort_model.broiler_info())
        k.update(timezone_info(timezone=timezone))
    except Exception as e:
        k = {"broiler_info": {},
             'status': 400, 'message': "Error: " + str(e)}
        k.update(mort_model.broiler_info())
        k.update(timezone_info(timezone=timezone))
    return jsonify(k)


class GrowthPredictions:

    def __init__(self, broilerID, start_date, end_date, current_date, cycle_id):

        self.broilerID = broilerID
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = current_date
        self.cycle_id = cycle_id

        self.weights_array = None
        self.weights_cohorts = None
        self.current_growth_array = None

        self.placement_date = None
        self.placement_weight = None

        self.m = None
        self.model_name = None
        self.cycles_len = 0

    def broiler_info(self):
        d = {
            'broiler_info': {
                'broiler_ID': str(self.broilerID),
                'start_date': self.start_date,
                'current_date': self.current_date,
                'end_date': self.end_date,
                'broiler_cycle_day': (
                        pd.to_datetime(self.current_date).date() - pd.to_datetime(self.start_date).date()).days,
                'cycle_id': str(self.cycle_id),
            }
        }
        return d

    def _load_historic_growth(self):

        db_url_yml = "https://f0d0d29c-7c5c-4212-9e05-6aa455b05adc-bluemix" \
                     ":e608b4ab60210ce0f9cf016e6d91efa45d515f3a57a2c2086b670f4cbdf30171@f0d0d29c-7c5c-4212-9e05-6aa455b05adc" \
                     "-bluemix.cloudantnosqldb.appdomain.cloud"
        couch = couchdb.Server(db_url_yml)
        the_cycles = {}
        if 'historical' not in couch:
            self.m = 'No historical data in all broiler houses'
        else:
            db = couch['historical']  # historical exists

            if str(self.broilerID) in db:  #
                doc = db[str(self.broilerID)]['file']  # whole excel file
                cycles = []
                for i in doc:
                    cycles.append(pd.DataFrame(doc[i]['doc']))  # cycles contains list of dataframes
                the_cycles[str(self.broilerID)] = cycles
                self.m = 'Local model was used'
                self.cycles_len = len(cycles)
                if len(the_cycles[str(self.broilerID)]) <= 10:  # not enough data, use global model

                    self.m = 'There is histocal data, but defaulting to global model because there is not enough historical data'
                    docs = db.view("_all_docs", include_docs=True)
                    self.cycles_len = 0
                    for i in docs:
                        i = i['doc']
                        if i['_id'].startswith('_'):
                            continue
                        doc = i['file']
                        cycles1 = []
                        for j in doc:
                            cycles1.append(pd.DataFrame(doc[j]['doc']))
                        the_cycles[str(i['_id'])] = cycles1  # cycles1 is a list of dataframes
                        self.cycles_len += len(cycles1)

            else:  # use all data, ie global model

                self.m = 'broilerID does not have historical data, defaulting to global model'
                docs = db.view("_all_docs", include_docs=True)
                self.cycles_len = 0
                for i in docs:
                    i = i['doc']
                    if i['_id'].startswith('_'):
                        continue
                    doc = i['file']
                    cycles1 = []
                    for j in doc:
                        cycles1.append(pd.DataFrame(doc[j]['doc']))
                    the_cycles[str(i['_id'])] = cycles1  # cycles1 is a list of dataframes
                    self.cycles_len += len(cycles1)

        weights_cohorts = {}
        weights_array = np.array([[-1, -1, -1, -1]])

        for broilerID_key in list(the_cycles.keys()):

            for cycle in range(len(the_cycles[broilerID_key])):
                weights_cohorts[broilerID_key] = the_cycles[broilerID_key][cycle].loc[
                    (the_cycles[broilerID_key][cycle]["AGE"] == 7) | (the_cycles[broilerID_key][cycle]["AGE"] == 14) | (
                            the_cycles[broilerID_key][cycle]["AGE"] == 21) | (
                            the_cycles[broilerID_key][cycle]["AGE"] == 28), ["WEIGHT"]]

                weights_array = np.append(weights_array, weights_cohorts[broilerID_key].values.transpose(), axis=0)

        weights_array = weights_array[np.all(weights_array != -1, axis=1)]  # remove rows with -1 weights
        weights_array = weights_array[np.all(weights_array != None, axis=1)]  # remove rows with None weights
        weights_array = weights_array[np.all(weights_array != 0, axis=1)]  # # remove rows with 0 weights

        self.weights_array = weights_array
        self.weights_cohorts = weights_cohorts

        return self

    def _get_current_growth_array(self):

        data = requests.post(f'{webapp}:178/getHistoricalWeight/', proxies={"http": None, "https": None, },
                             json={"id": self.broilerID, "start": self.start_date,
                                   "end": self.current_date + "T23:59:59"}).json()
        data_df = pd.DataFrame(data)

        if len(data_df) == 0:

            self.current_growth_array = data_df

        else:
            data_df = data_df.sort_values(by=["dateTime"])

            current_growth_array = data_df["weightAverage"].values  # / data_df["sampleSize"].values
            self.current_growth_array = np.reshape(current_growth_array, (1, -1))

        return self

    def _get_model(self, i, pred_data):

        # used Linear regression to make prediction
        X = self.weights_array[:, 0:i]
        y = np.reshape(self.weights_array[:, i], (-1, 1))

        X_scaler = StandardScaler()
        y_scaler = StandardScaler()

        X_scaler.fit(X)
        y_scaler.fit(y)

        X = X_scaler.transform(X)
        y = y_scaler.transform(y)

        poly = PolynomialFeatures(1)
        X = poly.fit_transform(X)
        if self.cycles_len >= 100:
            a = AutoAI()
            model = a.model_selection(X, y)
            model.fit(X, y)
        else:
            model = LinearRegression(fit_intercept=True).fit(X, y)  # sm.OLS(y, X).fit()
        self.model_name = type(model).__name__
        pred_data = X_scaler.transform(pred_data)
        prediction = model.predict(poly.fit_transform(pred_data))
        prediction = y_scaler.inverse_transform(prediction)
        prediction = prediction.reshape(prediction.shape[0], 1)
        return prediction

    def fit_model(self):

        self._load_historic_growth()  # Loading historical data from Cloudant
        self._get_current_growth_array()

        # get number of growth in in cycle
        no_growths = self.current_growth_array.shape[1]

        predictions_dict = {}

        if self.m:
            predictions_dict["predictions"] = {'message': self.m}

        else:
            predictions_dict["predictions"] = {}

        if no_growths == 0:  # No data has been collected, ie meaning we not at day 7 yet. No modelling here, used ratios from Michael to predict because there was not historical data with day 0

            data_placement = requests.get(f'{webapp}:178/getPlacementWeights/' + str(self.broilerID),
                                          proxies={"http": None, "https": None, }).json()
            data_placement_df = pd.DataFrame(data_placement)
            data_placement_df['date'] = pd.to_datetime(data_placement_df['date'])
            data_placement_df = data_placement_df.sort_values(by="date")
            data_placement_df = data_placement_df.loc[data_placement_df["date"] <= self.current_date + "T23:59:59"]
            placement_weight = data_placement_df.iloc[-1]["totalBirdWeight"] / data_placement_df.iloc[-1][
                "noOfBirdsPerBox"]

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=7)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())] = {}

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=7)).date())][
                "broiler_cycle_day"] = 7
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())][
                "broiler_cycle_day"] = 14
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                "broiler_cycle_day"] = 21
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "broiler_cycle_day"] = 28
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "broiler_cycle_day"] = 35

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=7)).date())][
                "prediction"] = placement_weight * 4.001
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=7)).date())][
                    "prediction"] * 2.656
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())][
                    "prediction"] * 2.021
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                    "prediction"] * 1.608
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                    "prediction"] * 1.341

        elif no_growths == 1:  # means day 7 has been recorded and the current day is either 7,8,9,10,11,12,13

            prediction_day_14 = self._get_model(1, self.current_growth_array)  # use day 7 only to predict day 14
            pred_data = np.array([self.current_growth_array, prediction_day_14]).flatten()
            pred_data = np.reshape(pred_data, (1, -1))
            prediction_day_21 = self._get_model(2, pred_data)  # use day 7, day 14 prediction to predict day 21

            pred_data = np.array([self.current_growth_array, prediction_day_14, prediction_day_21]).flatten()
            pred_data = np.reshape(pred_data, (1, -1))
            prediction_day_28 = self._get_model(3, pred_data)  # use day 7, day 14, 21 predictions to predict day 28

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())] = {}

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())][
                "broiler_cycle_day"] = 14
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                "broiler_cycle_day"] = 21
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "broiler_cycle_day"] = 28
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "broiler_cycle_day"] = 35

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=14)).date())][
                "prediction"] = prediction_day_14.tolist()[0][0]
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                "prediction"] = prediction_day_21.tolist()[0][0]
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "prediction"] = prediction_day_28.tolist()[0][0]

            # for day 35 we are not predicting because we dont have day 35 in historical data, therefore we use ratio form michael and multipy it with day 28 prediction to get estimate
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                    "prediction"] * 1.341

        elif no_growths == 2:  # means day 7 and 14 has been recorded and the current day is either 14,15,16,17,18,19,20

            prediction_day_21 = self._get_model(2, self.current_growth_array)  # use day 7, day 14 to predict day 21

            pred_data = np.array(
                [self.current_growth_array[0][0], self.current_growth_array[0][1], prediction_day_21]).flatten()
            pred_data = np.reshape(pred_data, (1, -1))
            prediction_day_28 = self._get_model(3,
                                                pred_data)  # use day 7, day 14 and the prediction of day 21 to predict day 28

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())] = {}

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                "broiler_cycle_day"] = 21
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "broiler_cycle_day"] = 28
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "broiler_cycle_day"] = 35

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=21)).date())][
                "prediction"] = prediction_day_21.tolist()[0][0]
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "prediction"] = prediction_day_28.tolist()[0][0]
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                    "prediction"] * 1.341

        elif no_growths == 3:  # means day 7 ,14,21 has been recorded and the current day is either 21-27

            prediction_day_28 = self._get_model(3, self.current_growth_array)  # use day 7,14,21 to predict day 28

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())] = {}
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())] = {}

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "broiler_cycle_day"] = 28
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "broiler_cycle_day"] = 35

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                "prediction"] = prediction_day_28.tolist()[0][0]
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "prediction"] = \
                predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=28)).date())][
                    "prediction"] * 1.341
        elif no_growths == 4:  # means day 7 ,14,21 has been recorded and the current day is either 21-27
            prediction_day_35 = self._get_model(3, self.current_growth_array[:,:3])  # use day 7,14,21, 28 to predict day 35
            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())] = {}

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "broiler_cycle_day"] = 35

            predictions_dict["predictions"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=35)).date())][
                "prediction"] = prediction_day_35.tolist()[0][0] * 1.341
        else:
            return {"status": 400,
                    "message": "Multiple weights have been recorded, please ensure that there not exists weight recordings for days 7, 14, 21 , 28 etc... per 7 day period only"}

        if self.model_name:
            predictions_dict["predictions"]['model_name'] = self.model_name
        return predictions_dict


@app.route("/v1/api/model/predictive/weight", methods=['POST'])
def weight():

    data = request.form.to_dict()

    broilerID = int(data.get('broilerID'))
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    current_date = data.get('current_date')
    cycle_id = data.get('cycle_id')
    timezone = data.get('timezone', 2)

    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return status
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return status
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return status
    if not current_date:
        status = {"status": 400, "message": "current_date missing"}
        return status
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return status
    if not timezone:
        status = {"status": 400, "message": "timezone missing"}
        return status
    growth_model = GrowthPredictions(broilerID=broilerID, start_date=start_date, end_date=end_date,
                                     current_date=current_date, cycle_id=cycle_id)

    try:
        growth_model._load_historic_growth()
        growth_model._get_current_growth_array()

        k = growth_model.fit_model()
        k.update(growth_model.broiler_info())
        k.update(timezone_info(timezone=timezone))
    except Exception as e:
        k = {"broiler_info": {},
             'status': 400, 'message': "Error: " + str(e)}
        k.update(growth_model.broiler_info())

        k.update(timezone_info(timezone=timezone))
    return jsonify(k)


