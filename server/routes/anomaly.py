import copy
import datetime
import pandas as pd
import requests
from flask import request, jsonify, Response

from server import credentials, app, webapp
from server.routes.index import connect_db


class Anomaly:
    def __init__(self, data):
        self.start_date = pd.to_datetime(data.get('start_date'))
        self.end_date = pd.to_datetime(data.get('end_date'))
        self.broilerID = data.get('broilerID')
        self.noChickens = data.get('noChickens')
        self.limit = int(data.get('limit'))
        self.getBriolerDetails = None
        self.mortality_target = 0.07
        self.weight_target = {
            'day7': 4.6,
            'day14': 2.656,
            'day21': 2.021,
            'day28': 1.608,
            'day35': 11.8,
        }

    def mortality(self):
        self.getBriolerDetails = requests.get(f'{webapp}:178/getBriolerDetails/{self.broilerID}')
        couch = connect_db(credentials['custom_url'])

        if 'historical' not in couch:
            m = 'No historical data in all broiler houses'
            return {'status': 400, 'message': m}
        else:
            db = couch['historical']  # historical exists
            docs = db.view("_all_docs", include_docs=True)

        x = {}
        for i in docs:
            i = i['doc']
            if i['_id'].startswith('_'):
                continue
            doc = i['file']
            for j in doc:
                d = pd.DataFrame(doc[j]['doc'])
                for k in d.iterrows():
                    k = k[1]
                    if k['AGE'] in x:
                        x[k['AGE']].append(k['MORT'])
                    else:
                        x[k['AGE']] = [k['MORT']]

        new_df = pd.concat([pd.DataFrame({f'{l}': x[l]}) for l in x], axis=1, )  # ignore_index=True
        mortality_data = []

        for i in self.getBriolerDetails.json()['tbl_BriolerData']:
            mortality_data.append({"broilerID": i['briolerID'], "fatalities": i['fatalities'], "culls": i['culls'],
                                   "date": i['dateTime']})

        mortality = pd.DataFrame(mortality_data)
        mortality["date"] = mortality["date"].apply(lambda x: pd.to_datetime(x).date())
        mortality = mortality.sort_values(by=["date"])

        mortality = mortality[
            (mortality["date"] <= self.end_date.date()) & (mortality["date"] >= self.start_date.date())]
        mortality["AGE"] = mortality["date"].apply(lambda x: (pd.to_datetime(x).date() - self.start_date.date()).days)
        mortality["MORT"] = mortality["fatalities"] + mortality["culls"]
        mortality["original_value"] = copy.deepcopy(mortality["fatalities"] + mortality["culls"])

        mortality = mortality.reset_index(drop=True)
        try:
            mortality['average'] = mortality['AGE'].apply(lambda x: round(new_df.mean()[x]))
        except KeyError:
            pass
        mortality['CUMM'] = None
        mortality['RATE'] = None
        mortality['deviation'] = None
        mortality['deviationP'] = None

        def fun(x, mortality):
            df_to_age = copy.deepcopy(mortality.loc[mortality['AGE'] <= x['AGE']])
            df_to_age['CUMM'] = df_to_age['MORT'].cumsum()
            cumsum = df_to_age.iloc[-1]['CUMM']
            rate = 100 * x['MORT'] / (int(self.noChickens) - (cumsum - x['MORT']))
            deviation = 0 if rate < self.mortality_target else abs(rate - self.mortality_target)
            deviationP = 0 if rate < self.mortality_target else 100 * abs(
                rate - self.mortality_target) / self.mortality_target
            if deviationP > self.limit:
                df_to_age.loc[len(df_to_age) - 1, 'MORT'] = x['average']
                df_to_age['CUMM'] = df_to_age['MORT'].cumsum()
                cumsum = df_to_age.iloc[-1]['CUMM']
                rate = 100 * x['MORT'] / (int(self.noChickens) - (cumsum - x['MORT']))
                # deviation = 0
                # deviationP = 0
                mortality.loc[mortality['AGE'] == x['AGE'], 'MORT'] = (x['average'])
            mortality.loc[mortality['AGE'] == x['AGE'], 'RATE'] = rate
            mortality.loc[mortality['AGE'] == x['AGE'], 'CUMM'] = cumsum
            mortality.loc[mortality['AGE'] == x['AGE'], 'deviation'] = deviation
            mortality.loc[mortality['AGE'] == x['AGE'], 'deviationP'] = deviationP

        [fun(i[1].to_dict(), mortality) for i in mortality.iterrows()]
        results = []
        for i in mortality.iterrows():
            temp = i[1].to_dict()
            if temp['deviationP'] > self.limit:
                results.append({
                    'age': temp['AGE'],
                    'original_value': temp['original_value'],
                    'new_value': temp['MORT'],
                    'deviationP': temp['deviationP']
                })
            else:
                results.append({
                    'age': temp['AGE'],
                    'original_value': temp['original_value'],
                    'deviationP': temp['deviationP']
                })

        # results = [i[1].to_dict() for i in mortality[['AGE', 'MORT', 'average', 'deviationP']].iterrows()]
        return results

    def weight(self):
        couch = connect_db(credentials['custom_url'])
        if 'historical' not in couch:
            m = 'No historical data in all broiler houses'
            return {'status': 400, 'message': m}
        else:
            db = couch['historical']  # historical exists
            docs = db.view("_all_docs", include_docs=True)

        x = {}
        for i in docs:
            i = i['doc']
            if i['_id'].startswith('_'):
                continue
            doc = i['file']
            for j in doc:
                d = pd.DataFrame(doc[j]['doc'])
                for k in d.iterrows():
                    k = k[1]
                    if k['AGE'] in x:
                        x[k['AGE']].append(k['WEIGHT'])
                    else:
                        x[k['AGE']] = [k['WEIGHT']]

        weights = pd.concat([pd.DataFrame({f'{l}': x[l]}) for l in x], axis=1, )
        w_mean = weights.dropna(axis=0, how='all', thresh=None, subset=None, inplace=False)
        w_mean = w_mean.dropna(axis=1, how='all', thresh=None, subset=None, inplace=False)
        w_mean = w_mean.mean().to_dict()

        weight_df = requests.post(f'{webapp}:178/getHistoricalWeight', data={'id': self.broilerID,
                                                                             'start': self.start_date.date().isoformat(),
                                                                             'end': self.end_date.date().isoformat()})
        if weight_df.status_code != 200:
            status = {'status': 400, 'message': 'Could not connect to the API: ' + str(weight_df.url)}
            return status
        elif not weight_df.json():
            status = {'status': 400, 'message': 'No weight data have been recorded'}
            return status
        weight_df = pd.DataFrame(weight_df.json())

        data_placement_df = requests.get(f'{webapp}:178/getPlacementWeights/{self.broilerID}')
        data_placement_df = pd.DataFrame(data_placement_df.json())
        if len(data_placement_df) == 0:
            status = {"status": 400, "message": f"Placement Weights have not been recorded on {self.start_date.date()}"}
            return status
        data_placement_df['date'] = data_placement_df['date'].apply(
            lambda a: pd.to_datetime(a, utc=True).date() if a else a)

        data_placement_df = data_placement_df.sort_values(by="date")
        temp_df = data_placement_df.loc[data_placement_df["date"] == self.start_date.date()]
        if len(temp_df) > 0:
            placement_weight = temp_df.iloc[-1]["totalBirdWeight"] / temp_df.iloc[-1]["noOfBirdsPerBox"]
        else:
            status = {"status": 400,
                      "message": f"No placement weight data for given start date: {self.start_date.date()}"}
            return status

        weight_df['dateTime'] = weight_df['dateTime'].apply(lambda x: pd.to_datetime(x).date() if x else x)
        weight_df = weight_df.sort_values(by=["dateTime"])

        weights_targets = {'7': placement_weight * self.weight_target['day7'],
                           '14': self.weight_target['day14'] * weight_df.loc[weight_df['dateTime'] == (
                                   self.start_date + datetime.timedelta(days=7)).date(), 'weightAverage'].tolist()[
                               0],
                           '21': self.weight_target['day21'] * weight_df.loc[weight_df['dateTime'] == (
                                   self.start_date + datetime.timedelta(days=14)).date(), 'weightAverage'].tolist()[
                               0],
                           '28': self.weight_target['day28'] * weight_df.loc[weight_df['dateTime'] == (
                                   self.start_date + datetime.timedelta(days=21)).date(), 'weightAverage'].tolist()[
                               0], }
        #        '35' = (self.weight_target['day35'] * day7weight) + (additional_weight * 7)

        # weight_data = pd.DataFrame(columns=['weight', 'target', 'average', 'deviation', 'deviationP'],
        #                            index=[7, 14, 21, 28])
        weight_data = {}
        for i in [7, 14, 21, 28]:
            date = self.start_date + datetime.timedelta(days=i)
            x = weight_df.loc[weight_df['dateTime'] == date.date()]
            average = w_mean[str(i)]
            target = weights_targets[str(i)]

            # deviation = 0 if average > target else abs(average - target)
            deviationP = 0 if average > target else 100 * abs(average - target) / target
            # 'target'] = target
            # weight_data.loc[i, 'average'] = average
            original_value = x['weightAverage'].values[0]

            weight_data[f'day{i}'] = {
                'original_value': x['weightAverage'].values[0],
                'deviationP': deviationP,
            }
            if deviationP > self.limit:
                weight_data[f'day{i}'] = {
                    'original_value': x['weightAverage'].values[0],
                    'new_value': average,
                    'deviationP': deviationP,
                }
        # return [{f'day{i[0]}': i[1].to_dict()} for i in weight_data[['weight', 'average', 'deviationP']].iterrows()]
        return weight_data


@app.route("/v1/api/model/anomaly/mortality", methods=['POST'])
def anomaly_mortality(data=None):
    data = request.form.to_dict()

    broilerID = data.get('broilerID')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    noChickens = data.get('noChickens')
    limit = data.get('limit')

    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return status
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return status
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return status
    if not noChickens:
        status = {"status": 400, "message": "noChickens missing"}
        return status
    if not limit:
        status = {"status": 400, "message": "limit missing"}
        return status
    try:
        a = Anomaly(data)
        status = a.mortality()
    except Exception as e:
        status = {'status': 400, 'message': f'An error has occurred. Error: {str(e)}'}
    if __name__ == '__main__':
        return jsonify(status)
    else:
        return status


@app.route("/v1/api/model/anomaly/mortality/get", methods=['POST'])
def get_anomaly_mortality(data=None):
    data = request.form.to_dict()

    broilerID = data.get('broilerID')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    noChickens = data.get('noChickens')
    limit = data.get('limit')

    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return status
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return status
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return status
    if not noChickens:
        status = {"status": 400, "message": "noChickens missing"}
        return status
    if not limit:
        status = {"status": 400, "message": "limit missing"}
        return status
    resp = anomaly_mortality(data)
    if resp and 'status' in resp:
        return jsonify(resp)
    df = pd.DataFrame(resp)
    df = df.fillna('')
    return Response(
        df.to_csv(index=None),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=MortalityAnomaly.csv"})


@app.route("/v1/api/model/anomaly/weight", methods=['POST'])
def anomaly_weight(data=None):
    data = request.form.to_dict()

    broilerID = data.get('broilerID')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    noChickens = data.get('noChickens')
    limit = data.get('limit')

    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return status
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return status
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return status
    if not noChickens:
        status = {"status": 400, "message": "noChickens missing"}
        return status
    if not limit:
        status = {"status": 400, "message": "limit missing"}
        return status
    try:
        a = Anomaly(data)
        status = a.weight()
    except Exception as e:
        status = {'status': 400, 'message': f'An error has occurred. Error: {str(e)}'}
    if __name__ == '__main__':
        return jsonify(status)
    else:
        return status


@app.route("/v1/api/model/anomaly/weight/get", methods=['POST'])
def get_anomaly_weight():
    data = request.form.to_dict()

    broilerID = data.get('broilerID')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    noChickens = data.get('noChickens')
    limit = data.get('limit')

    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return status
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return status
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return status
    if not noChickens:
        status = {"status": 400, "message": "noChickens missing"}
        return status
    if not limit:
        status = {"status": 400, "message": "limit missing"}
        return status
    resp = anomaly_weight(data)
    if resp and 'status' in resp:
        return jsonify(resp)
    df = pd.DataFrame(resp).T
    df = df.fillna('')
    return Response(
        df.to_csv(),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=WeightAnomaly.csv"})
