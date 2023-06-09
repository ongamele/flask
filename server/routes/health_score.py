# Class for the omniolytics health score
from datetime import timedelta
from datetime import datetime
from itertools import chain
from collections import Counter

import pytz
import requests
import pandas as pd
import numpy as np
from flask import request, jsonify

from server import app, credentials, webapp
from server.routes.index import connect_db
from server.routes.practice import Practice, FCR, timezone_info


class HealthScore:
    def __init__(self, data=None):
        self.data = data
        self.base = f'{webapp}:178'
        self.red = 'Red'
        self.amber = 'Amber'
        self.green = 'Green'
        self.broilerID = data.get('broilerID')
        self.start_date = pd.to_datetime(data.get('start_date'), utc=True)
        self.end_date = pd.to_datetime(data.get('end_date'), utc=True)
        self.current_date = pd.to_datetime(data.get('current_date'), utc=True)
        self.noChickens = str(data.get('noChickens', '1')).strip()
        self.noChickens = int(self.noChickens)
        self.cycle_id = data.get('cycle_id')
        self.strain_id = data.get('strain_id')
        self.timezone = data.get('timezone', 2)
        self.timezone = int(self.timezone)
        # 2.Activities
        # 2.1. Vaccination
        self.ph_actual_values = None
        self.ph_sani_condition = None
        self.ph_sani_best_practice_limits = None
        self.ph_va_condition = None
        self.ph_va_best_practice_limits = None

        # 2.2. Mortalities

        # 2.3. Sanitation
        self.orp_condition = None
        self.orp_actual_values = None
        self.orp_best_practice_limits = None
        # 2.4. Feed Recorded
        # self.feed = requests.get(f'{self.base}/getFeed/{self.broilerID}')
        self.feed = requests.get(f'{self.base}/getWaterFeedData/{self.broilerID}/')
        # 2.5. Weight Recorded

        self.master = {}
        self.detail = {}
        # broiler weights
        self.getBroilerWeights = requests.get(f'{self.base}/getBroilerWeights/{self.broilerID}')

        # placement dates
        self.getPlacementWeights = requests.get(f'{self.base}/getPlacementWeights/{self.broilerID}')

        # broiler details
        self.getBriolerDetails = requests.get(f'{self.base}/getBriolerDetails/{self.broilerID}')

        # catch data
        self.catch_data = requests.get(f'{self.base}/getCatchData/{self.broilerID}')

        # latest
        # self.latest = requests.post(f'https://local-insights.eu-gb.mybluemix.net/v1/api/practice/latest', data=data)

        self.practice = Practice()
        self.practice.strain_id = self.strain_id
        self.strain = self.practice.get_strain_file()

        if self.start_date > self.current_date:
            self.day = -1
        else:
            self.day = (self.current_date - self.start_date).days

        # self.day = min(self.day, len(self.strain.keys())-2)  # fix maximum days to 35
        # Get cycles
        # self.cycles = requests.get(f'{self.base}/getCylces')
        # self.cycle_name = None

        # initialise cycle id and name
        # self.get_cycles()

        # todo HACK REMOVE
        # self.cycle_id = 8
        # self.cycle_name = 'Broiler Cycle 2'
        # HACK END

        # activities data
        self.getActData = requests.get(f'{self.base}/getActData/{self.broilerID}/{self.cycle_id}')

        # catch day
        self.catch_day = None
        self.getBroilerSettings = requests.get(f'{self.base}/getBroilerSettings/{self.broilerID}')

        # sensor uniformity targets
        self.sensor_target = {
            'green': 1,
            'red': 5
        }
        # Lux target
        self.lux_sensor_target = {
            'green': 18,
            'red': 20
        }
        # KPI target for weight
        self.kpi_weight_target = {
            'day7': 4.6,
            'day14': 2.656,
            'day21': 2.021,
            'day28': 1.608,
            'day35': 11.8,
        }

        # KPI FCR Targets
        self.kpi_fcr_target = {
            'day0_21': 1.4,
            'day22_35': 1.8,
        }
        # FCR model
        self.fcr_model = FCR(broilerID=self.broilerID, noChickens=self.noChickens,
                             start_date=self.start_date.date().isoformat(),
                             end_date=self.end_date.date().isoformat(),
                             current_date=self.current_date.date().isoformat(),
                             cycle_id=self.cycle_id)

        # default values
        self.kpi_mortality_target = {
            'green': 0.05,
            'red': 0.07
        }
        # weight uniformity
        self.uniformity_weight_target = {
            'green': 8,
            'red': 10
        }
        self.initialise()

    # def get_cycles(self, ):
    #     if self.cycles.status_code == 200:
    #         df = pd.DataFrame(self.cycles.json())
    #         if len(df) > 0:
    #             x = df.loc[df.broilerid == self.broilerID, ['id', 'name']]
    #             x = x.sort_values(by='id')
    #             if len(x) > 0:
    #                 self.cycle_id = x.iloc[-1]['id']
    #                 self.cycle_name = x.iloc[-1]['name']

    def broiler_info(self):
        d = {
            'broiler_info': {
                'broiler_ID': str(self.broilerID),
                'start_date': self.start_date.date().isoformat(),
                'current_date': self.current_date.date().isoformat(),
                'end_date': self.end_date.date().isoformat(),
                'broiler_cycle_day': self.day,
                'cycle_id': str(self.cycle_id),
                'strain_id': self.strain_id
            },
        }
        d.update(timezone_info(timezone=self.timezone))
        return d

    def initialise(self):
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in server:
            db = server['health_targets']
            if str(self.broilerID) in db:
                doc = db[str(self.broilerID)]
                temp = doc['kpi_weight_target']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.kpi_weight_target = temp

                temp = doc['kpi_mortality_target']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.kpi_mortality_target = {
                    'green': temp['green'],
                    'amber': [temp['green'], temp['red']],
                    'red': temp['red']
                }
                temp = doc['sensor_target']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.sensor_target = {
                    'green': temp['green'],
                    'amber': [temp['green'], temp['red']],
                    'red': temp['red']
                }

                temp = doc['lux_sensor_target']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.lux_sensor_target = {
                    'green': temp['green'],
                    'amber': [temp['green'], temp['red']],
                    'red': temp['red']
                }

                temp = doc['uniformity_weight_target']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.uniformity_weight_target = {
                    'green': temp['green'],
                    'amber': [temp['green'], temp['red']],
                    'red': temp['red']
                }

                temp = doc['kpi_fcr']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.kpi_fcr_target = temp
        catch_data_df = pd.DataFrame(self.getBroilerSettings.json())
        try:
            catch_data_df["dateFinalCatch"] = catch_data_df["dateFinalCatch"].apply(
                lambda i: pd.to_datetime(i, utc=True).date() if i else i)
        except:
            self.catch_day = {'message': 'Catch date is not recorded'}
        catch_data_df = catch_data_df.loc[
            (self.start_date.date() < catch_data_df.dateFinalCatch) & (catch_data_df.dateFinalCatch <= self.end_date.date())]
        if len(catch_data_df) == 0:
            self.catch_day = {'message': 'Catch date is not recorded'}
        elif len(catch_data_df) > 1:
            catch_day = catch_data_df['dateFinalCatch'].values[0]
            self.catch_day = {'message': f'Multiple catch dates are recorded. There should only be one catch date. Recorded '
                                 f'dates are {[str(i) for i in catch_data_df["dateFinalCatch"].values.tolist()]}. '
                                 f'Selected first date',
                      'date': catch_day}
        elif len(catch_data_df) == 1:
            catch_day = catch_data_df['dateFinalCatch'].values[0]
            self.catch_day = {'message': f'Catch date recorded', 'date': catch_day}

    def conditions(self):
        """Function to calculate the conditions of sanitation and vaccination"""

        self.orp_best_practice_limits = self.strain[f'day {self.day}']['water quality']['ORP']['value']  # float
        # list
        self.ph_sani_best_practice_limits = self.strain[f'day {self.day}']['water quality']['PH sanitizing']['value']
        # list
        self.ph_va_best_practice_limits = self.strain[f'day {self.day}']['water quality']['PH vaccination']['value']

        if float(self.orp_actual_values) > float(self.orp_best_practice_limits):
            self.orp_condition = True
        else:
            self.orp_condition = False
        if min(self.ph_sani_best_practice_limits) < float(self.ph_actual_values) < max(self.ph_sani_best_practice_limits):
            self.ph_sani_condition = True
        else:
            self.ph_sani_condition = False
        if min(self.ph_va_best_practice_limits) < float(self.ph_actual_values) < max(self.ph_va_best_practice_limits):
            self.ph_va_condition = True
        else:
            self.ph_va_condition = False

    def kpi_weight(self, ):
        """KPI """

        if self.getPlacementWeights.status_code != 200:
            return {"kpi_weight": [{"message": 'Could not connect to the API: ' + self.getPlacementWeights.url}]}
        elif not self.getPlacementWeights.json():
            return {"kpi_weight": [{"message": f"Placement Weights have not been recorded on {self.current_date.date()}"}]}

        if self.getBroilerWeights.status_code != 200:
            return {"kpi_weight": [{"message": 'Could not connect to the API: ' + self.getBroilerWeights.url}]}
        elif not self.getBroilerWeights.json():
            return {"kpi_weight": [{"message": f'Weights have not been recorded on {self.current_date.date()}'}]}

        data_placement_df = pd.DataFrame(self.getPlacementWeights.json())
        if len(data_placement_df) == 0:
            status = {"kpi_weight": [{"message": f"Placement Weights have not been recorded on "
                                                 f"{self.current_date.date()}"}]}
            return status
        data_placement_df['date'] = \
            data_placement_df['date'].apply(lambda a: pd.to_datetime(a, utc=True).date() if a else a)

        data_placement_df = data_placement_df.sort_values(by="date")

        # get placement date for a given start date
        temp_df = data_placement_df.loc[data_placement_df["date"] == self.start_date.date()]
        if len(temp_df) > 0:
            # latest_placement_date = temp_df.iloc[-1]['date']
            placement_weight = temp_df.iloc[-1]["totalBirdWeight"] / temp_df.iloc[-1]["noOfBirdsPerBox"]
        else:
            status = {
                "kpi_weight": [{"message": f"No placement weight data for given start date: {self.start_date.date()}"}]
            }
            return status

        # Obsolete algorithm
        # temp_df = data_placement_df.loc[data_placement_df["date"] <= self.current_date]
        # if len(temp_df) > 0:
        #     latest_placement_date = temp_df.iloc[-1]['date']
        #     placement_weight = temp_df.iloc[-1]["totalBirdWeight"] / temp_df.iloc[-1]["noOfBirdsPerBox"]
        # else:
        #     latest_placement_date = data_placement_df.iloc[-1]['date']
        #     placement_weight = data_placement_df.iloc[-1]["totalBirdWeight"] /
        #     data_placement_df.iloc[-1]["noOfBirdsPerBox"]

        # Get weight
        weight_df = pd.DataFrame(self.getBroilerWeights.json())
        if len(weight_df) == 0:
            status = {"kpi_weight": [{"message": f"Weights have not been recorded on {self.current_date.date()}"}]}
            return status

        d = {'kpi_weight': []}

        # If day is between 0-6, no need to get weights
        if self.day in range(0, 7):
            # dt = self.current_date.date()
            d['kpi_weight'] = [{
                # "date": dt.isoformat(),
                "day": self.day,
                "status": self.green,
                'placement_weight': placement_weight,
                "message": "Weight performance will be provided from day 7"
            }]
            self.master.update({"kpi_weight": [self.green]})
            return d

        weight_df['dateTime'] = weight_df['dateTime'].apply(lambda a: pd.to_datetime(a, utc=True).date() if a else a)
        weight_df = weight_df.sort_values(by="dateTime")
        weight_df = weight_df.set_index('dateTime')
        weight_df = weight_df[['weightAverage']]

        # weight dates
        # day7 = latest_placement_date + timedelta(days=7)
        # day35 = latest_placement_date + timedelta(days=35)
        indicator_day7 = lambda y: [self.green, y - target_weight7] if y > target_weight7 else (
            [self.amber, 0] if y == target_weight7 else [self.red, 0])  # calculate status
        indicator_day14 = lambda y: self.green if y > target_weight14 else \
            (self.amber if y == target_weight14 else self.red)  # calculate status
        indicator_day21 = lambda y: self.green if y > target_weight21 else \
            (self.amber if y == target_weight21 else self.red)  # calculate status
        indicator_day28 = lambda y: self.green if y > target_weight28 else \
            (self.amber if y == target_weight28 else self.red)  # calculate status
        indicator_day35 = lambda y: self.green if y > target_weight35 else (
            self.amber if y == (target_weight35 + additional_weight * 7) else self.red)  # calculate status

        try:
            self.detail['kpi_weight'] = []
            # day 7 target
            if self.day in range(7, 14):
                target_weight7 = placement_weight * self.kpi_weight_target['day7']
                day = (self.start_date + timedelta(days=7)).date()
                w = weight_df.loc[day].tolist()[0]
                status, additional_weight = indicator_day7(w)
                deviation = 0
                deviationp = 0
                if status == self.red:
                    deviation = round(abs(target_weight7 - w), 2)
                    deviationp = round(abs(target_weight7 - w) / target_weight7 * 100, 2) if target_weight7 != 0 else 0
                d['kpi_weight'] = [{
                    "date": day.isoformat(),
                    "day": 7,
                    "status": status,
                    "additional_weight": float(additional_weight),
                    "weight_target": float(target_weight7),
                    "weight_target_ratio": float(self.kpi_weight_target['day7']),
                    "weight_average": float(w),
                    "deviation": float(deviation),
                    "deviationp": float(deviationp),
                }]
                if status in [self.red, self.amber]:
                    self.detail.update(d)

                self.master.update({'kpi_weight': [indicator_day7(w)[0]]})  # set day 7 status
            elif self.current_date == self.end_date:
                if self.catch_data.status_code != 200:
                    return {"kpi_weight": [{"message": 'Could not connect to the API: ' + self.catch_data.url}]}

                if not self.catch_data.json():
                    self.master.update({"kpi_weight": [self.red]})
                    return {
                        "kpi_weight": [{
                            'status': [self.red],
                            "message": f'No catch data recorded  on {self.current_date.date()}'
                        }]
                    }
                catch_data_df = pd.DataFrame(self.catch_data.json())
                catch_data_df["date"] = catch_data_df["date"].apply(lambda x: pd.to_datetime(x, utc=True).date())
                catch_data_df = catch_data_df.set_index('date')
                target_weight7 = placement_weight * self.kpi_weight_target['day7']
                try:
                    day7weight = weight_df.loc[self.start_date.date() + timedelta(days=7)].tolist()[0]  # day 7 weight
                    _, additional_weight = indicator_day7(day7weight)

                    #  (11.8 * d7_wt) + ( 7 * d7add_wt)
                    target_weight35 = (self.kpi_weight_target['day35'] * day7weight) + (additional_weight * 7)

                    catch_total_weight = catch_data_df.loc[self.end_date.date()]['totalweight']
                    catch_no_of_birds = catch_data_df.loc[self.end_date.date()]['noofbirds']
                    x = catch_total_weight/catch_no_of_birds
                    status = indicator_day35(x)
                    deviation = 0
                    deviationp = 0
                    if status == self.red:
                        deviation = round(abs(target_weight35 - x), 2)
                        deviationp = round(abs(target_weight35 - x) / target_weight35 * 100, 2) if target_weight35 != 0 else 0
                    d['kpi_weight'].append({
                        "date": self.current_date.date().isoformat(),
                        "day": self.day,
                        "status": status,
                        "additional_weight": additional_weight,
                        "weight_target": float(target_weight35),
                        "weight_target_ratio": float(self.kpi_weight_target['day35']),
                        "deviation": float(deviation),
                        "deviationp": float(deviationp),
                        "weight_average": float(x)
                    })
                    self.master.update({'kpi_weight': [indicator_day35(x)]})
                except KeyError as e:
                    status = {
                        "kpi_weight": [{"message": f"No catch weight data recorded on {self.current_date.date()}"}]
                    }
                    return status

            # day 14 target
            elif self.day in range(14, 21):
                day = (self.start_date + timedelta(days=14)).date()
                w = weight_df.loc[day].tolist()[0]  # today's weight (day14)
                target_weight14 = self.kpi_weight_target['day14'] * weight_df.loc[day - timedelta(days=7)].tolist()[0]

                status = indicator_day14(w)
                deviation = 0
                deviationp = 0
                if status == self.red:
                    deviation = round(abs(target_weight14 - w), 2)
                    deviationp = round(abs(target_weight14 - w) / target_weight14 * 100,
                                       2) if target_weight14 != 0 else 0
                d['kpi_weight'] = [{
                    "date": day.isoformat(),
                    "day": 14,
                    "status": status,
                    "weight_target": float(target_weight14),
                    "weight_target_ratio": float(self.kpi_weight_target['day14']),
                    "weight_average": float(w),
                    "deviation": float(deviation),
                    "deviationp": float(deviationp),
                }]
                if status in [self.red, self.amber]:
                    self.detail.update(d)

                self.master.update({'kpi_weight': [indicator_day14(w)]})

            # day 21 target
            elif self.day in range(21, 28):
                day = (self.start_date + timedelta(days=21)).date()
                w = weight_df.loc[day].tolist()[0]  # today's weight (day21)
                target_weight21 = self.kpi_weight_target['day21'] * weight_df.loc[day - timedelta(days=7)].tolist()[0]

                status = indicator_day21(w)
                deviation = 0
                deviationp = 0
                if status == self.red:
                    deviation = round(abs(target_weight21 - w), 2)
                    deviationp = round(abs(target_weight21 - w) / target_weight21 * 100,
                                       2) if target_weight21 != 0 else 0
                d['kpi_weight'] = [{
                    "date": day.isoformat(),
                    "day": 21,
                    "status": status,
                    "weight_target": float(target_weight21),
                    "weight_target_ratio": float(self.kpi_weight_target['day21']),
                    "deviation": float(deviation),
                    "deviationp": float(deviationp),
                    "weight_average": float(w)
                }]
                if status in [self.red, self.amber]:
                    self.detail.update(d)

                self.master.update({'kpi_weight': [indicator_day21(w)]})

            # day 28 target
            elif self.day in range(28, 35):
                day = (self.start_date + timedelta(days=28)).date()
                w = weight_df.loc[day].tolist()[0]  # today's weight (day28)
                target_weight28 = self.kpi_weight_target['day28'] * weight_df.loc[day - timedelta(days=7)].tolist()[0]

                status = indicator_day28(w)
                deviation = 0
                deviationp = 0
                if status == self.red:
                    deviation = round(abs(target_weight28 - w), 2)
                    deviationp = round(abs(target_weight28 - w) / target_weight28 * 100,
                                       2) if target_weight28 != 0 else 0
                d['kpi_weight'] = [{
                    "date": day.isoformat(),
                    "day": 28,
                    "status": status,
                    "weight_target": float(target_weight28),
                    "weight_target_ratio": float(self.kpi_weight_target['day28']),
                    "weight_average": float(w),
                    "deviation": float(deviation),
                    "deviationp": float(deviationp),
                }]
                if status in [self.red, self.amber]:
                    self.detail.update(d)

                self.master.update({'kpi_weight': [indicator_day28(w)]})

            # day 35 target
            elif self.day >= 35:
                try:
                    day7weight = weight_df.loc[self.start_date.date() + timedelta(days=7)].tolist()[0]  # day 7 weight
                    target_weight7 = placement_weight * self.kpi_weight_target['day7']
                    _, additional_weight = indicator_day7(day7weight)

                    target_weight35 = (self.kpi_weight_target['day35'] * day7weight) + (additional_weight * 7)
                    day = (self.start_date + timedelta(days=35)).date()

                    x = weight_df.loc[day].tolist()[0]  # day 35 weight
                except KeyError:
                    status = {
                        "kpi_weight": [{"message": f"No weight data recorded on {self.current_date.date().isoformat()}"}]
                    }
                    return status
                status = indicator_day35(x)
                deviation = 0
                deviationp = 0
                if status == self.red:
                    deviation = round(abs(target_weight35 - x), 2)
                    deviationp = round(abs(target_weight35 - x) / target_weight35 * 100,
                                       2) if target_weight35 != 0 else 0
                d['kpi_weight'].append({
                    "date": day.isoformat(),
                    "day": self.day,
                    "status": status,
                    "additional_weight": float(additional_weight),
                    "weight_target": float(target_weight35),
                    "weight_target_ratio": float(self.kpi_weight_target['day35']),
                    "weight_average": float(x),
                    "deviation": float(deviation),
                    "deviationp": float(deviationp),
                })
                if status in [self.red, self.amber]:
                    self.detail.update(d)

                self.master.update({'kpi_weight': [status]})  # set day 35 status
        except KeyError:
            status = {"kpi_weight": [{"message": f"No weight data recorded on {self.current_date.date()}."}]}
            return status

        return d

        # 2. Activities
        # 2.1 Vaccination

    def act_vaccination_record(self):
        self.conditions()
        if self.getActData.status_code != 200:
            return {"act_vaccination_record": [{"message": 'Could not connect to the API: ' + self.getActData.url}]}
        elif not self.getActData.json():
            return {"act_vaccination_record": [{"message": f'No vaccination data has been recorded on {self.current_date.date()}'}]}

        if self.ph_actual_values is None:
            status = {"act_vaccination_record": [{"message": f"Could not get current PH sensor value for broiler {self.broilerID}"}]}
            return status
        if self.orp_actual_values is None:
            status = {"act_vaccination_record": [{"message": f"Could not get current ORP sensor value for broiler {self.broilerID}"}]}
            return status

        df = pd.DataFrame(self.getActData.json())

        df['startDate'] = df['startDate'].apply(lambda x: pd.to_datetime(x, utc=True) if x else x)
        df['completedDateTime'] = df['completedDateTime'].apply(lambda x: pd.to_datetime(x, utc=True) if x else x)
        df['completedBitTimestamp'] = df['completedBitTimestamp'].apply(
            lambda x: pd.to_datetime(x, utc=True) if x else "None")

        d = {'act_vaccination_record': []}
        self.master['act_vaccination_record'] = []
        self.detail['act_vaccination_record'] = []

        try:
            v = df[(df.startDate >= self.current_date) & (
                    df.startDate < self.current_date + timedelta(hours=23, minutes=59))]
            for i in v.iterrows():
                i = i[1]
                today = datetime.now(pytz.utc)
                if i['typeAct'] == 1 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] < i['completedBitTimestamp']:
                    d['act_vaccination_record'].append({
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination is completed not within scheduled time',
                        'status': self.red,
                    })
                    self.master['act_vaccination_record'].append(self.red)
                    self.detail['act_vaccination_record'].append({
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination is completed not within scheduled time',
                        'status': self.red,
                    })
                    #  check if scheduled date has passed
                elif i['typeAct'] == 1 and not i['completed'] and i['completedBitTimestamp'] is "None" \
                        and today > i['completedDateTime']:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'],
                        'message': f'Vaccination is not completed within scheduled time. There are {today-i["completedDateTime"]} passed.',
                        'status': self.red,
                    }
                    d['act_vaccination_record'].append(temp)
                    self.master['act_vaccination_record'].append(self.red)
                    self.detail['act_vaccination_record'].append(temp)
                    #
                elif i['typeAct'] == 1 and not i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] < i['completedBitTimestamp']:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination date has passed and vaccination is not completed',
                        'status': self.red
                    }
                    d['act_vaccination_record'].append(temp)
                    self.master['act_vaccination_record'].append(self.red)
                    self.detail['act_vaccination_record'].append(temp)
                elif i['typeAct'] == 1 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and self.ph_va_condition and \
                        self.orp_condition:

                    d['act_vaccination_record'].append({
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination is completed within scheduled time and conditions are met',
                        'status': self.green,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_va_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    })
                    self.master['act_vaccination_record'].append(self.green)
                elif i['typeAct'] == 1 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and not self.ph_va_condition and \
                        not self.orp_condition:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination is completed within scheduled time'
                                   ' but both PH and ORP conditions are not met',
                        'status': self.amber,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_va_best_practice_limits
                        },
                        'ORP_condition':{
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    }
                    d['act_vaccination_record'].append(temp)
                    self.master['act_vaccination_record'].append(self.amber)
                    self.detail['act_vaccination_record'].append(temp)
                elif i['typeAct'] == 1 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and self.ph_va_condition and \
                        not self.orp_condition:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination is completed within scheduled time,'
                                   ' and ORP conditions are not met',
                        'status': self.amber,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_va_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    }
                    d['act_vaccination_record'].append(temp)
                    self.master['act_vaccination_record'].append(self.amber)
                    self.detail['act_vaccination_record'].append(temp)
                elif i['typeAct'] == 1 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and not self.ph_va_condition and \
                        self.orp_condition:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Vaccination is completed within scheduled time,'
                                   ' and PH conditions are not met',
                        'status': self.amber,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_va_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    }
                    d['act_vaccination_record'].append(temp)
                    self.master['act_vaccination_record'].append(self.amber)
                    self.detail['act_vaccination_record'].append(temp)
        except Exception:
            status = {"act_vaccination_record": []}
            return status
        return d

    # 2.2 Mortality

    def act_mortality_record(self, ):

        if self.getBriolerDetails.status_code != 200:
            return {"act_mortality_record": [{"message": 'Could not connect to the API :' + self.getBriolerDetails.url}]}
        elif not self.getBriolerDetails.json():
            status = {"act_mortality_record": [{'status': [self.red],
                                                "message": f'Mortality is not recorded on {self.current_date.date()}'}]}
            self.master.update({'act_feed_record': [self.red]})
            return status
        df = pd.DataFrame(self.getBriolerDetails.json()['tbl_BriolerData'])
        if len(df) == 0:
            status = {"act_mortality_record": [{'status': [self.red],
                                                "message": f'Mortality is not recorded on {self.current_date.date()}'}]}
            self.master.update({'act_feed_record': [self.red]})
            return status
        df['dateTime'] = df['dateTime'].apply(lambda x: pd.to_datetime(x, utc=True).date())
        df = df.set_index('dateTime')
        self.detail['act_mortality_record'] = []
        if self.current_date.date() not in df.index:
            status = {'act_mortality_record': [{'status': [self.red], 'message': 'Mortality not recorded'}]}
            self.master.update({'act_mortality_record': [self.red]})
            self.detail.update(status)

        elif df.at[self.current_date.date(), 'tbl_MortalitiesEvent']:
            status = {'act_mortality_record': [{'status': [self.green], 'message': "Mortality recorded"}]}
            self.master.update({'act_mortality_record': [self.green]})

        else:
            status = {'act_mortality_record': [{'status': [self.amber],
                                                'message': 'Mortality recorded with no reason'}]}
            self.master.update({'act_mortality_record': [self.amber]})
            self.detail.update(status)

        return status

    # 2.3 Sanitation

    def act_sanitation_record(self):
        self.conditions()
        if self.getActData.status_code != 200:
            return {"act_sanitation_record": [{"message": 'Could not connect to the API: ' + self.getActData.url}]}
        elif not self.getActData.json():
            return {"act_sanitation_record": [{"message": f"No sanitation data recorded on {self.current_date.date()}"}]}
        if self.ph_actual_values is None:
            status = {"act_vaccination_record": [{
                "message": f"Could not get current PH sensor value for broiler {self.broilerID}"}]}
            return status
        if self.orp_actual_values is None:
            status = {"act_sanitation_record": [{
                "message": f"Could not get current ORP sensor value for broiler {self.broilerID}"}]}
            return status
        df = pd.DataFrame(self.getActData.json())
        if len(df) == 0:
            status = {"act_sanitation_record": [{
                "message": f"No sanitation data recorded on {self.current_date.date()}"}]}
            return status
        df['startDate'] = df['startDate'].apply(lambda x: pd.to_datetime(x, utc=True) if x else x)

        df['completedDateTime'] = df['completedDateTime'].apply(lambda x: pd.to_datetime(x, utc=True) if x else x)

        df['completedBitTimestamp'] = df['completedBitTimestamp'].apply(
            lambda x: pd.to_datetime(x, utc=True) if x else "None")

        d = {'act_sanitation_record': []}
        self.master['act_sanitation_record'] = []
        self.detail['act_sanitation_record'] = []

        try:
            v = df[(df.startDate >= self.current_date) & (
                    df.startDate < self.current_date + timedelta(hours=23, minutes=59))]
            for i in v.iterrows():
                i = i[1]
                today = datetime.now(pytz.utc)

                if i['typeAct'] == 2 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] < i['completedBitTimestamp']:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Sanitation is completed not within scheduled time',
                        'status': self.red,
                    }
                    d['act_sanitation_record'].append(temp)
                    self.master['act_sanitation_record'].append(self.red)
                    self.detail['act_sanitation_record'].append(temp)
                    #  check if scheduled date has passed
                elif i['typeAct'] == 2 and not i['completed'] and i['completedBitTimestamp'] is "None" \
                        and today > i['completedDateTime']:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'],
                        'message': f'Sanitation is not completed within scheduled time. There are {today-i["completedDateTime"]} passed.',
                        'status': self.red,
                    }
                    d['act_sanitation_record'].append(temp)
                    self.master['act_sanitation_record'].append(self.red)
                    self.detail['act_sanitation_record'].append(temp)
                    #
                elif i['typeAct'] == 2 and not i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] < i['completedBitTimestamp']:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Sanitation date has passed and sanitation is not completed',
                        'status': self.red
                    }
                    d['act_sanitation_record'].append(temp)
                    self.master['act_sanitation_record'].append(self.red)
                    self.detail['act_sanitation_record'].append(temp)
                elif i['typeAct'] == 2 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] \
                        and self.orp_condition and self.ph_sani_condition:

                    d['act_sanitation_record'].append({
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Sanitation is completed within scheduled time and conditions are met',
                        'status': self.green,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_sani_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    })
                    self.master['act_sanitation_record'].append(self.green)
                elif i['typeAct'] == 2 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and not self.orp_condition \
                        and not self.ph_sani_condition:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Sanitation is completed within scheduled time but both PH and ORP '
                                   'conditions are not met',
                        'status': self.amber,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_sani_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    }
                    d['act_sanitation_record'].append(temp)
                    self.master['act_sanitation_record'].append(self.amber)
                    self.detail['act_sanitation_record'].append(temp)
                elif i['typeAct'] == 2 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and not self.orp_condition \
                        and self.ph_sani_condition:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Sanitation is completed within scheduled time and ORP '
                                   'conditions are not met',
                        'status': self.amber,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_sani_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    }
                    d['act_sanitation_record'].append(temp)
                    self.master['act_sanitation_record'].append(self.amber)
                    self.detail['act_sanitation_record'].append(temp)
                elif i['typeAct'] == 2 and i['completed'] and i['completedBitTimestamp'] is not "None" \
                        and i['completedDateTime'] >= i['completedBitTimestamp'] and self.orp_condition \
                        and not self.ph_sani_condition:
                    temp = {
                        'start_date': i['startDate'].isoformat(),
                        'schedule_date': i['completedDateTime'].isoformat(),
                        'completed_date': i['completedBitTimestamp'].isoformat(),
                        'message': 'Sanitation is completed within scheduled time and PH '
                                   'conditions are not met',
                        'status': self.amber,
                        'PH_condition': {
                            'actual_value': float(self.ph_actual_values),
                            'target': self.ph_sani_best_practice_limits
                        },
                        'ORP_condition': {
                            'actual_value': float(self.orp_actual_values),
                            'target': self.orp_best_practice_limits
                        }
                    }
                    d['act_sanitation_record'].append(temp)
                    self.master['act_sanitation_record'].append(self.amber)
                    self.detail['act_sanitation_record'].append(temp)
        except Exception:
            status = {"act_sanitation_record": []}
            return status
        return d

    # 2.4 Feed Recorded

    def act_feed_record(self):

        if self.feed.status_code != 200:
            return {"act_feed_record": [{"message": 'Could not connect to the API: ' + self.feed.url}]}
        elif not self.feed.json():
            status = {"act_feed_record": [{'status': [self.red],
                                           "message": f"No feed data recorded on {self.current_date.date()}"}]}
            self.master.update({'act_feed_record': [self.red]})
            return status

        df = pd.DataFrame(self.feed.json())
        if len(df) == 0:
            status = {"act_feed_record": [{'status': [self.red],
                                           "message": f"No feed data recorded on {self.current_date.date()}"}]}
            self.master.update({'act_feed_record': [self.red]})
            return status
        df['dateTime'] = df['dateTime'].apply(lambda x: pd.to_datetime(x, utc=True).date() if x else x)

        df = df.set_index('dateTime')
        df = df.sort_index()
        try:
            if self.current_date.date() not in df.index:
                status = {"act_feed_record": [{'status': [self.red], 'message': 'Feed is not recorded'}]}
                self.master.update({'act_feed_record': [self.red]})
                self.detail.update(status)
            elif df.loc[self.current_date.date(), ["feed"]].isnull().values.any():
                status = {"act_feed_record": [{'status': [self.red], 'message': 'Feed is not recorded'}]}
                self.master.update({'act_feed_record': [self.red]})
                self.detail.update(status)
            else:
                status = {"act_feed_record": [{'status': [self.green], 'message': 'Feed is recorded'}]}
                self.master.update({'act_feed_record': [self.green]})
        except KeyError:
            status = {"act_feed_record": [{'status': [self.red], 'message': 'Feed is not recorded'}]}
            self.master.update({'act_feed_record': [self.red]})
            self.detail.update(status)
            pass
        return status

    # 2.5 Weight Recorded

    def act_weight_record(self):

        if self.getBroilerWeights.status_code != 200:
            return {"act_weight_record": [{"message": 'Could not connect to the API: ' + self.getBroilerWeights.url}]}
        elif not self.getBroilerWeights.json():
            status = {"act_weight_record": [{'status': [self.red],
                                             "message": f'No weight data recorded on {self.current_date.date()}'}]}
            self.master.update({'act_weight_record': [self.red]})
            self.detail.update(status)
            return status

        df = pd.DataFrame(self.getBroilerWeights.json())
        if len(df) == 0:
            status = {"act_weight_record": [{'status': [self.red],
                                             "message": f"No weight data recorded on {self.current_date.date()}"}]}
            self.master.update({'act_weight_record': [self.red]})
            self.detail.update(status)
            return status
        df['dateTime'] = df['dateTime'].apply(lambda i: pd.to_datetime(i, utc=True).date() if i else i)

        df = df.set_index('dateTime')
        df = df.sort_index()

        back = ['weightBack', 'weightBackR']
        front = ['weightFront', 'weightFrontR']
        center = ['weightCenter', 'weightCenterR']

        if 'date' in self.catch_day:
            if self.current_date.date() == self.catch_day['date']:
                status = {'act_weight_record': [{'status': [self.green], 'message': "Catch weight recorded"}]}
                self.master.update({'act_weight_record': [self.green]})
                return status

        weightmultiples = 7
        if self.day % weightmultiples != 0:
            nf = int(weightmultiples * np.floor(self.day / weightmultiples))
            dt = (self.start_date + timedelta(days=nf))
            # print(dt)
            try:
                if nf in range(0, 7):
                    status = {'act_weight_record': [{'status': [self.green],
                                                     'message': "Weights will be checked after day 7"}]}
                    self.master.update({'act_weight_record': [self.green]})
                    return status

                if dt.date() not in df.index:
                    status = {'act_weight_record': [{'status': [self.red],
                                                     'message': "Weight not recorded"}]}
                    self.master.update({'act_weight_record': [self.red]})

                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.red],
                                                     'message': 'Weight not recorded in all sections'}]}
                    self.master.update({'act_weight_record': [self.red]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.green],
                                                     'message': 'Weight recorded in all sections'}]}
                    self.master.update({'act_weight_record': [self.green]})

                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in back and center sections'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() >1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in back and front sections'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in front and center sections'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() >1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in front section'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in back section'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in center section'}]}
                    self.master.update({'act_weight_record': [self.amber]})
            except KeyError:
                status = {'act_weight_record': [{'status': [self.red],
                                                 'message': f'No weight data recorded on {self.current_date.date()}'}]}
                self.master.update({'act_weight_record': [self.red]})
        else:
            '''If self.day is 7,14,21,28,35,...'''
            try:
                dt = self.current_date
                if self.day in range(0, 7):
                    status = {'act_weight_record': [{'status': [self.green],
                                                     'message': "Weight performance will be provided from day 7"}]}
                    self.master.update({'act_weight_record': [self.green]})
                    return status

                if self.current_date.date() not in df.index:
                    status = {'act_weight_record': {'status': [self.red],
                                                    'message': "Weight not recorded"}}
                    self.master.update({'act_weight_record': [self.red]})
                    return status
                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.red],
                                                     'message': 'Weight not recorded in all sections'}]}
                    self.master.update({'act_weight_record': [self.red]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.green],
                                                     'message': 'Weight recorded in all sections'}]}
                    self.master.update({'act_weight_record': [self.green]})

                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in back and center sections'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in back and front sections'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in front and center sections'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() > 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in front section'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() > 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() <= 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in back section'}]}
                    self.master.update({'act_weight_record': [self.amber]})

                elif df.loc[dt.date(), back].isnull().values.sum() <= 1 and df.loc[
                    dt.date(), front].isnull().values.sum() <= 1 and df.loc[dt.date(), center].isnull().values.sum() > 1:
                    status = {'act_weight_record': [{'status': [self.amber],
                                                     'message': 'Weight not recorded in center section'}]}
                    self.master.update({'act_weight_record': [self.amber]})

            except KeyError:
                status = {'act_weight_record': [{"message": f"No weight data recorded on {self.current_date.date()}"}]}
                self.master.update({'act_weight_record': [self.red]})
        return status

    # 3. Uniformity
    # 3.1 Weight uniformity
    def weight_uniformity(self, ):
        """Calculate weight uniformity"""

        if self.getBroilerWeights.status_code != 200:
            return {"weight_uniformity": [{"message": 'Could not connect to the API: ' + self.getBroilerWeights.url}]}
        elif not self.getBroilerWeights.json():
            return {"weight_uniformity": [{"message": f'No weight data recorded on {self.current_date.date()}'}]}

        df = pd.DataFrame(self.getBroilerWeights.json())
        if len(df) == 0:
            status = {"weight_uniformity": [{"message": f'No weight data recorded on {self.current_date.date()}'}]}
            return status

        if self.day in range(0, 7):
            dt = self.current_date.date()
            d = {'weight_uniformity': [{
                "date": dt.isoformat(),
                "day": self.day,
                "status": self.green,
                "message": "Weight uniformity performance will be provided from day 7"
            }]}
            self.master.update({"weight_uniformity": [self.green]})
            return d

        elif self.current_date == self.end_date:

            if self.catch_data.status_code != 200:
                return {"weight_uniformity": [{"message": 'Could not connect to the API: ' + self.catch_data.url}]}

            if not self.catch_data.json():
                self.master.update({"weight_uniformity": [self.red]})
                return {"weight_uniformity": {'status': [self.red],
                                              "message": f'No catch data recorded  on {self.current_date.date()}'}}

            catch_data_df = pd.DataFrame(self.catch_data.json())
            catch_data_df["date"] = catch_data_df["date"].apply(lambda p: pd.to_datetime(p, utc=True).date())
            catch_data_df = catch_data_df.set_index('date')
            try:
                temp = catch_data_df.loc[self.current_date.date()]

                status = {
                    'weight_uniformity': [{
                        'status': [self.green],
                        'message': f"Catch weight recorded on {self.current_date.date()}"
                    }]
                }
                self.master.update({'weight_uniformity': [self.green]})
                return status

            except KeyError as e:
                '''if catch data doesn't return anything OR date doesn't exist in the data'''
                status = {
                    'weight_uniformity': [{
                        'status': [self.red],
                        'message': f"Catch weight not recorded on {self.current_date.date()}"
                    }]
                }
                self.master.update({'weight_uniformity': [self.red]})
                return status

        df['dateTime'] = df['dateTime'].apply(lambda x: pd.to_datetime(x, utc=True).date() if x else x)
        df = df.set_index('dateTime')
        df = df[['weightBack', 'weightCenter', 'weightFront', 'weightBackR', 'weightCenterR', 'weightFrontR']]

        weightmultiples = 7
        if self.day % weightmultiples != 0:
            nf = int(weightmultiples * np.floor(self.day/weightmultiples))
            dt = (self.start_date + timedelta(days=nf))
            try:
                weight_data = df.loc[dt.date()]
                cv = lambda y: np.std(y, ddof=1) / np.mean(y) * 100  # calculate CV
                weight_coef_variation = cv(weight_data)

                indicator = lambda y: self.green if 0 < y <= self.uniformity_weight_target['green'] else \
                    (self.amber if self.uniformity_weight_target['green'] < y < self.uniformity_weight_target['red']
                     else self.red)  # calculate status

                d = {
                    'weight_uniformity': [{
                        'status': indicator(weight_coef_variation),
                        'variation': weight_coef_variation,
                        'variation_target': self.uniformity_weight_target
                    }]
                }

                self.master.update({'weight_uniformity': [indicator(weight_coef_variation)]})
            except KeyError:
                status = {'weight_uniformity': {"status": [self.red],
                                                'message': f'No weight data found on {dt.date()}'}}  # "No weight data for date:: " + str(e)}
                return status
        else:
            try:
                weight_data = df.loc[self.current_date.date()]
                cv = lambda y: np.std(y, ddof=1) / np.mean(y) * 100  # calculate CV
                weight_coef_variation = cv(weight_data)
                # weight_coef_variation = variation(weight_data, axis = 1)

                indicator = lambda y: self.green if 0 < y <= self.uniformity_weight_target['green'] else \
                    (self.amber if self.uniformity_weight_target['green'] < y < self.uniformity_weight_target['red']
                     else self.red)  # calculate status

                d = {
                    'weight_uniformity': [{
                        'status': indicator(weight_coef_variation),
                        'variation': weight_coef_variation,
                        'variation_target': self.uniformity_weight_target
                    }]
                }

                self.master.update({'weight_uniformity': [indicator(weight_coef_variation)]})
            except KeyError:
                status = {"weight_uniformity": []}  # "No weight data for date:: " + str(e)}
                return status
        return d

    def kpi_mortality(self, ):
        """Calculate Mortality Rate"""

        if self.getBriolerDetails.status_code != 200:
            return {"kpi_mortality": [{"message": 'Could not connect to API: ' + self.getBriolerDetails.url}]}
        elif not self.getBriolerDetails.json():
            return {"kpi_mortality": [{"message": f'No mortality data recorded on {self.current_date.date()}'}]}

        df = pd.DataFrame(self.getBriolerDetails.json()['tbl_BriolerData'])
        if len(df) == 0:
            status = {"kpi_mortality": [{f"No mortality data recorded on {self.current_date.date()}"}]}
            return status
        df['dateTime'] = df['dateTime'].apply(lambda x: pd.to_datetime(x, utc=True).date() if x else x)
        df = df.sort_values(by='dateTime')

        try:
            df = df.loc[
                (df.dateTime >= self.start_date.date()) & (df.dateTime <= self.current_date.date()), ['fatalities',
                                                                                                      'culls',
                                                                                                      'dateTime']]
            df = df.set_index('dateTime')
            total_mortality = df.sum().sum()
            mortality_today = df.loc[self.current_date.date()].sum()
        except KeyError as e:
            status = {"kpi_mortality": [{"message": f'No mortality data recorded on {self.current_date.date()}'}]}
            return status
        mortality_rate = round((mortality_today / (self.noChickens - (total_mortality - mortality_today))) * 100, 2)

        indicator = lambda y: self.green if 0 < y <= self.kpi_mortality_target['green'] else (
            self.amber if self.kpi_mortality_target['green'] < y < self.kpi_mortality_target['red']
            else self.red)  # calculate status
        w = indicator(mortality_rate)
        deviation = 0
        deviationp = 0
        if w in [self.amber, self.red] and mortality_rate > 0:
            deviation = round(abs(mortality_rate - self.kpi_mortality_target['green']), 2)
            deviationp = round(abs(mortality_rate - self.kpi_mortality_target['green']) /
                               self.kpi_mortality_target['green'] * 100, 2) if self.kpi_mortality_target['green'] != 0 else 0
        elif w in [self.red, self.amber] and mortality_rate == 0:
            deviation = 100
            deviationp = 100
        d = {
            'kpi_mortality': [{
                'status': w,
                'mortality_today': float(mortality_today),
                'mortality_rate': float(mortality_rate),
                'mortality_target': self.kpi_mortality_target,
                'deviation': float(deviation),
                'deviationp': float(deviationp)
            }]
        }
        if indicator(mortality_rate) in [self.red, self.amber]:
            self.detail.update(d)

        self.master.update({'kpi_mortality': [indicator(mortality_rate)]})

        return d

    # 3.2 Sensors uniformity
    def sensor_uniformity(self, ):
        """Calculate sensor uniformity"""

        resp = self.practice.get_latest(self.data)
        if 'status' in resp and resp['status'] == 400:
            return resp

        sensors = {}
        for i in resp.keys():
            if i.startswith('SC') or i.startswith('AQS'):
                for j in resp[i]['actual_values']:
                    if j in sensors:
                        if j == 'Dark period':
                            sensors[j].append(resp[i]['actual_values'][j]['value'])
                        else:
                            sensors[j].append(resp[i]['actual_values'][j])
                    else:
                        if j == 'Dark period':
                            sensors[j] = [resp[i]['actual_values'][j]['value']]
                        else:
                            sensors[j] = [resp[i]['actual_values'][j]]
        cv = lambda y: np.std(y, ddof=1) / np.mean(y) * 100  # calculate CV
        indicator = lambda y: self.green if y <= self.sensor_target['green'] else (
            self.amber if self.sensor_target['green'] < y < self.sensor_target['red'] else self.red)
        indicator_lux = lambda y: self.green if y <= self.lux_sensor_target['green'] else (
            self.amber if self.lux_sensor_target['green'] < y < self.lux_sensor_target['red'] else self.red)
        d = {'sensor_uniformity': []}
        self.master.update({'sensor_uniformity': []})
        self.detail.update({'sensor_uniformity': []})
        for i in sensors:
            if len(sensors[i]) < 3:
                d['sensor_uniformity'].append({"sensor_unit": i, 'variation_target': self.lux_sensor_target,
                                               'message': f'Minimum three {i} sensor units are required. Currently have'
                                                          f' {len(sensors[i])} sensor unit(s)'})
                continue
            if i == 'Lux':
                self.master['sensor_uniformity'].append(indicator_lux(cv(sensors[i])))
                d['sensor_uniformity'].append({"sensor_unit": i, "status": indicator_lux(cv(sensors[i])),
                                               "variation": None if np.isnan(cv(sensors[i])) else cv(sensors[i]),
                                               'variation_target': self.lux_sensor_target
                                               })
                if indicator_lux(cv(sensors[i])) in [self.red, self.amber]:
                    self.detail['sensor_uniformity'].append({"sensor_unit": i, "status": indicator_lux(cv(sensors[i])),
                                                             "variation":None if np.isnan(cv(sensors[i])) else cv(sensors[i]),
                                                             'variation_target': self.lux_sensor_target
                                                             })
            else:
                self.master['sensor_uniformity'].append(indicator(cv(sensors[i])))
                d['sensor_uniformity'].append({"sensor_unit": i, "status": indicator(cv(sensors[i])),
                                               "variation": None if np.isnan(cv(sensors[i])) else cv(sensors[i]),
                                               'variation_target': self.sensor_target
                                               })
                if indicator(cv(sensors[i])) in [self.red, self.amber]:
                    self.detail['sensor_uniformity'].append({"sensor_unit": i, "status": indicator(cv(sensors[i])),
                                                             "variation": None if np.isnan(cv(sensors[i])) else cv(sensors[i]),
                                                             'variation_target': self.sensor_target
                                                             })
        return d

    # 4. Live Environmental Conditions
    def environmental_conditions(self):

        d = self.practice.get_latest(self.data)
        if 'status' in d and d['status'] == 400:
            return d
        self.strain = self.practice.strain_file  # save strain file
        self.master.update({'environmental_conditions': []})
        self.detail.update({'environmental_conditions': []})
        results = {}
        for k in d:
            if k in ['broiler_info', 'timestamp_name', 'timestamp']:
                continue

            actual_values = d[k]['actual_values']
            indicators = d[k]['indicators']
            best_practice_limits = d[k]['best_practice_limits']
            output = {}
            for i in indicators:
                if i == 'ORP':
                    self.orp_actual_values = actual_values[i]
                if i == 'PH':
                    self.ph_actual_values = actual_values[i]

                if isinstance(best_practice_limits[i], (int, float)):
                    if indicators[i] == 0:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.red}
                        self.master['environmental_conditions'].append(self.red)
                        self.detail['environmental_conditions'].append(results_)
                        output[i] = results_
                    else:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.green}
                        self.master['environmental_conditions'].append(self.green)
                        output[i] = results_
                elif 'fuzzy_upper_bounds' in indicators[i] and 'fuzzy_lower_bounds' in indicators[i]:
                    if actual_values[i] < min(best_practice_limits[i]):
                        if indicators[i]['fuzzy_lower_bounds']['fuzzy_lower_value'] == 0:
                            results_ = {'sensor_id': k,
                                        'sensor_unit': i,
                                        'reading': actual_values[i],
                                        'target': best_practice_limits[i],
                                        'status': self.red}
                            output[i] = results_
                            self.master['environmental_conditions'].append(self.red)
                            self.detail['environmental_conditions'].append(results_)

                    elif actual_values[i] > max(best_practice_limits[i]):
                        if indicators[i]['fuzzy_upper_bounds']['fuzzy_upper_value'] == 0:
                            results_ = {'sensor_id': k,
                                        'sensor_unit': i,
                                        'reading': actual_values[i],
                                        'target': best_practice_limits[i],
                                        'status': self.red}
                            output[i] = results_
                            self.master['environmental_conditions'].append(self.red)
                            self.detail['environmental_conditions'].append(results_)
                    elif indicators[i]['fuzzy_upper_bounds']['fuzzy_upper_value'] == 1 or indicators[i]['fuzzy_lower_bounds']['fuzzy_lower_value'] == 1:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.green}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.green)
                    else:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.amber}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.amber)
                        self.detail['environmental_conditions'].append(results_)

                elif 'fuzzy_upper_bounds' in indicators[i]:
                    if indicators[i]['fuzzy_upper_bounds']['fuzzy_upper_value'] == 0:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.red}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.red)
                        self.detail['environmental_conditions'].append(results_)
                    elif indicators[i]['fuzzy_upper_bounds']['fuzzy_upper_value'] in [1, None]:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.green}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.green)

                    else:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.amber}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.amber)
                        self.detail['environmental_conditions'].append(results_)
                elif 'fuzzy_lower_bounds' in indicators[i]:
                    if indicators[i]['fuzzy_lower_bounds']['fuzzy_lower_value'] == 0:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.red}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.red)
                        self.detail['environmental_conditions'].append(results_)

                    elif indicators[i]['fuzzy_lower_bounds']['fuzzy_lower_value'] in [1, None]:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.green}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.green)

                    else:
                        results_ = {'sensor_id': k,
                                    'sensor_unit': i,
                                    'reading': actual_values[i],
                                    'target': best_practice_limits[i],
                                    'status': self.amber}
                        output[i] = results_
                        self.master['environmental_conditions'].append(self.amber)
                        self.detail['environmental_conditions'].append(results_)

            results[k] = output
        d = {'environmental_conditions': results}

        return d

    # 5. Sensor devices
    def sensor_devices(self):
        """Sync with other API
        Any 3 sensors down in house - Red
        Any 1-2 sensors down in house – Amber
        All sensors working – Green
        """

        self.practice.devices()
        devices = self.practice.get_device_details
        briolerName = {}
        for i in devices.json():
            if i['briolerName'] in briolerName:
                briolerName[i['briolerName']].append(i['unit'])
            else:
                briolerName[i['briolerName']] = [i['unit']]
        response = {}
        counter = {}
        for i in briolerName:
            d = Counter(briolerName[i])
            counter[i] = d
            if i is None:
                continue
            if 'Offline' in d:
                if d['Offline'] > 2:
                    response[i] = [self.red]
                    continue
                elif d['Offline'] > 0:
                    response[i] = [self.amber]
                    continue
                else:
                    response[i] = [self.green]
                    continue
            if 'Online' in d:
                response[i] = [self.green]

        name = self.practice.get_broiler_name(self.broilerID)
        self.master['sensor_devices'] = response.get(name)
        d = {
            'sensor_devices': [{
                'status': response.get(name),
                'devices': counter.get(name)
            }]
        }
        if response.get(name) in [self.red, self.amber]:
            self.detail.update(d)
        return d

        # 6 FCR
    def kpi_fcr(self):
        d = {'kpi_fcr': []}
        if self.day < 7:
            d['kpi_fcr'] = [{
                "status": self.green,
                "message": "FCR performance will be provided from day 7",
                "fcr_target": self.kpi_fcr_target['day0_21'],
            }]
            self.master.update({'kpi_fcr': [self.green]})
            return d
        try:
            fcr_data = self.fcr_model.compute_FCRs()
            if not fcr_data or not 'FCRs' in fcr_data:
                return {"kpi_fcr": [{"message": f'FCR has not been computed on {self.current_date.date()}'}]}
        except Exception as e:
            return {"kpi_fcr": [{"message": f'FCR has not been computed on {self.current_date.date()}'}]}

        df = pd.DataFrame(fcr_data['FCRs'])
        df = df.T

        indicator_day0_21 = lambda y: self.green if 0 < y < self.kpi_fcr_target['day0_21'] else (
            self.amber if y == self.kpi_fcr_target['day0_21'] else self.red)  # calculate status
        indicator_day22_35 = lambda y: self.green if 0 < y < self.kpi_fcr_target['day22_35'] else (
            self.amber if y == self.kpi_fcr_target['day22_35'] else self.red)  # calculate status

        try:
            if self.day < 14:
                dt = (self.start_date + timedelta(days=7)).date()
                fcr = df['FCR value'].loc[str(dt)]
                if fcr == 0:
                    d = {'kpi_fcr': [{
                        "day": 7,
                        "status": self.red,
                        "message": f"Consumption data is missing for day {self.current_date.date()}",
                        "fcr_target": self.kpi_fcr_target['day0_21'],
                    }]}
                    self.master.update({'kpi_fcr': [self.red]})
                    self.detail.update(d)
                    return d
                status = indicator_day0_21(fcr)
                deviation = 0
                deviationp = 0
                if status in [self.red, self.amber] and fcr > 0:
                    deviation = round(abs(fcr - self.kpi_fcr_target['day0_21']), 2)
                    deviationp = round(abs(fcr - self.kpi_fcr_target['day0_21']) /
                                       self.kpi_fcr_target['day0_21'] * 100, 2)
                elif status in [self.red, self.amber] and fcr == 0:
                    deviation = 100
                    deviationp = 100
                d['kpi_fcr'] = [{
                    "date": dt.isoformat(),
                    "day": 7,
                    "status": status,
                    "fcr": fcr,
                    "fcr_target": self.kpi_fcr_target['day0_21'],
                    'deviation': float(deviation),
                    'deviationp': float(deviationp)
                }]
                self.master.update({'kpi_fcr': [indicator_day0_21(fcr)]})
                if status in [self.red, self.amber]:
                    self.detail.update(d)

            elif self.day < 21:
                dt = (self.start_date+ timedelta(days=14)).date()
                fcr = df['FCR value'].loc[str(dt)]
                if fcr == 0:
                    d = {'kpi_fcr': [{
                        "day": 14,
                        "status": self.red,
                        "message": f"Consumption data is missing for day {self.current_date.date()}",
                        "fcr_target": self.kpi_fcr_target['day0_21'],
                    }]}
                    self.master.update({'kpi_fcr': [self.red]})
                    self.detail.update(d)
                    return d
                status = indicator_day0_21(fcr)
                deviation = 0
                deviationp = 0
                if status in [self.red, self.amber] and fcr > 0:
                    deviation = round(abs(fcr - self.kpi_fcr_target['day0_21']), 2)
                    deviationp = round(abs(fcr - self.kpi_fcr_target['day0_21']) /
                                       self.kpi_fcr_target['day0_21'] * 100, 2)
                elif status in [self.red, self.amber] and fcr == 0:
                    deviation = 100
                    deviationp = 100
                d['kpi_fcr'] = [{
                    "date": dt.isoformat(),
                    "day": 14,
                    "status": status,
                    "fcr": fcr,
                    "fcr_target": self.kpi_fcr_target['day0_21'],
                    'deviation': float(deviation),
                    'deviationp': float(deviationp)
                }]
                self.master.update({'kpi_fcr': [indicator_day0_21(fcr)]})
                if status in [self.red, self.amber]:
                    self.detail.update(d)
            elif self.day < 28:
                dt = (self.start_date + timedelta(days=21)).date()
                fcr = df['FCR value'].loc[str(dt)]
                if fcr == 0:
                    d = {'kpi_fcr': [{
                        "day": 21,
                        "status": self.red,
                        "message": f"Consumption data is missing for day {self.current_date.date()}",
                        "fcr_target": self.kpi_fcr_target['day0_21'],
                    }]}
                    self.master.update({'kpi_fcr': [self.red]})
                    self.detail.update(d)
                    return d
                status = indicator_day0_21(fcr)
                deviation = 0
                deviationp = 0
                if status in [self.red, self.amber] and fcr > 0:
                    deviation = round(abs(fcr - self.kpi_fcr_target['day0_21']), 2)
                    deviationp = round(abs(fcr - self.kpi_fcr_target['day0_21']) /
                                       self.kpi_fcr_target['day0_21'] * 100, 2)
                elif status in [self.red, self.amber] and fcr == 0:
                    deviation = 100
                    deviationp = 100
                d['kpi_fcr'] = [{
                    "date": dt.isoformat(),
                    "day": 21,
                    "status": status,
                    "fcr": fcr,
                    "fcr_target": self.kpi_fcr_target['day0_21'],
                    'deviation': float(deviation),
                    'deviationp': float(deviationp),
                }]
                self.master.update({'kpi_fcr': [indicator_day0_21(fcr)]})
                if status in [self.red, self.amber]:
                    self.detail.update(d)
            elif self.day < 35:
                dt = (self.start_date + timedelta(days=28)).date()
                fcr = df['FCR value'].loc[str(dt)]
                if fcr == 0:
                    d = {'kpi_fcr': [{
                        "day": 28,
                        "status": self.red,
                        "message": f"Consumption data is missing for day {self.current_date.date()}",
                        "fcr_target": self.kpi_fcr_target['day22_35'],
                    }]}
                    self.master.update({'kpi_fcr': [self.red]})
                    self.detail.update(d)
                    return d
                status = indicator_day22_35(fcr)
                deviation = 0
                deviationp = 0
                if status in [self.red, self.amber] and fcr > 0:
                    deviation = round(abs(fcr - self.kpi_fcr_target['day22_35']), 2)
                    deviationp = round(abs(fcr - self.kpi_fcr_target['day22_35']) /
                                       self.kpi_fcr_target['day22_35'] * 100, 2)
                elif status in [self.red, self.amber] and fcr == 0:
                    deviation = 100
                    deviationp = 100
                d['kpi_fcr'] = [{
                    "date": dt.isoformat(),
                    "day": 28,
                    "status": status,
                    "fcr": fcr,
                    "fcr_target": self.kpi_fcr_target['day22_35'],
                    'deviation': float(deviation),
                    'deviationp': float(deviationp),
                }]
                self.master.update({'kpi_fcr': [indicator_day22_35(fcr)]})
                if status in [self.red, self.amber]:
                    self.detail.update(d)
            elif self.day >= 35:
                dt = (self.start_date + timedelta(days=35)).date()
                fcr = df['FCR value'].loc[str(dt)]
                if fcr == 0:
                    d = {'kpi_fcr': [{
                        "day": 35,
                        "status": self.red,
                        "message": f"Consumption data is missing for day {self.current_date.date()}",
                        "fcr_target": self.kpi_fcr_target['day22_35'],
                    }]}
                    self.master.update({'kpi_fcr': [self.red]})
                    self.detail.update(d)
                    return d
                status = indicator_day22_35(fcr)
                deviation = 0
                deviationp = 0
                if status in [self.red, self.amber] and fcr > 0:
                    deviation = round(abs(fcr - self.kpi_fcr_target['day22_35']), 2)
                    deviationp = round(abs(fcr - self.kpi_fcr_target['day22_35']) /
                                       self.kpi_fcr_target['day22_35'] * 100, 2)
                elif status in [self.red, self.amber] and fcr == 0:
                    deviation = 100
                    deviationp = 100
                d['kpi_fcr'] = [{
                    "date": dt.isoformat(),
                    "day": 35,
                    "status": status,
                    "fcr": fcr,
                    "fcr_target": self.kpi_fcr_target['day22_35'],
                    'deviation': float(deviation),
                    'deviationp': float(deviationp),
                }]
                self.master.update({'kpi_fcr': [indicator_day22_35(fcr)]})
                if status in [self.red, self.amber]:
                    self.detail.update(d)
        except KeyError as e:
            status = {"kpi_fcr": []}  # "No FCR data exists for date:: " + str(e)}
            return status
        return d

    def health(self, ):
        #
        d = self.broiler_info()
        sensor_uniformity = self.sensor_uniformity()
        if isinstance(sensor_uniformity, dict) and 'status' in sensor_uniformity and sensor_uniformity['status'] == 400:
            return sensor_uniformity

        details = dict(sensor_uniformity=sensor_uniformity,
                       sensor_devices=self.sensor_devices(),
                       environmental_conditions=self.environmental_conditions(),
                       kpi_mortality=self.kpi_mortality(),
                       weight_uniformity=self.weight_uniformity(),
                       kpi_weight=self.kpi_weight(),
                       act_vaccination_record=self.act_vaccination_record(),
                       act_sanitation_record=self.act_sanitation_record(),
                       act_mortality_record=self.act_mortality_record(),
                       act_feed_record=self.act_feed_record(),
                       act_weight_record=self.act_weight_record(),
                       kpi_fcr=self.kpi_fcr())

        status = Counter(list(chain.from_iterable(self.master.values())))
        if status['Red'] > 0:
            d['health'] = self.red
        elif status['Amber'] > 0:
            d['health'] = self.amber
        else:
            d['health'] = self.green
        d['details'] = details
        return d


@app.route('/v1/api/health', methods=['POST'])
def api_device():
    data = request.form.to_dict()

    if not data.get('broilerID'):
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    if not data.get('strain_id'):
        status = {"status": 400, "message": "strain_id missing"}
        return jsonify(status), 400
    if not data.get('start_date'):
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    if not data.get('end_date'):
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    if not data.get('current_date'):
        status = {"status": 400, "message": "current_date missing"}
        return jsonify(status), 400
    if not data.get('noChickens'):
        status = {"status": 400, "message": "noChickens missing"}
        return jsonify(status), 400
    if not data.get('cycle_id'):
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400
    if not data.get('parent_flock_age'):
        status = {"status": 400, "message": "parent_flock_age missing"}
        return jsonify(status), 400
    h = HealthScore(data=data)
    try:
        health = h.health()
        return jsonify(health)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        status.update(h.broiler_info())
        return jsonify(status)


@app.route('/v1/api/health/targets/get', methods=['POST'])
def targets_get():
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                return jsonify(doc)
            else:
                status = {'status': 400, 'message': f'No targets data available for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': 'No targets data available'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/kpi/weight/create', methods=['POST'])
def kpi_weight_create():
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    day7 = data.get('day7')
    if not day7:
        status = {"status": 400, "message": "day7 missing"}
        return jsonify(status), 400
    else:
        day7 = float(day7)
    day14 = data.get('day14')
    if not day14:
        status = {"status": 400, "message": "day14 missing"}
        return jsonify(status), 400
    else:
        day14 = float(day14)

    day21 = data.get('day21')
    if not day21:
        status = {"status": 400, "message": "day21 missing"}
        return jsonify(status), 400
    else:
        day21 = float(day21)

    day28 = data.get('day28')
    if not day28:
        status = {"status": 400, "message": "day28 missing"}
        return jsonify(status), 400
    else:
        day28 = float(day28)

    day35 = data.get('day35')
    if not day35:
        status = {"status": 400, "message": "day35 missing"}
        return jsonify(status), 400
    else:
        day35 = float(day35)

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                doc.update({
                    'kpi_weight_target': dict(day7=day7, day14=day14, day21=day21, day28=day28, day35=day35)
                })
                db.save(doc)
                status = {'status': 200, 'message': f'Updated weight KPI for broiler ID: {broilerID}'}
                return jsonify(status)
            else:
                db[str(broilerID)] = {
                    'kpi_weight_target': dict(day7=day7, day14=day14, day21=day21, day28=day28, day35=day35)
                }
                status = {'status': 200, 'message': f'Created weight KPI for broiler ID: {broilerID}'}
                return jsonify(status)
        else:
            server.create("health_targets")
            db = server['health_targets']
            db[str(broilerID)] = {
                'kpi_weight_target': dict(day7=day7, day14=day14, day21=day21, day28=day28, day35=day35)
            }
            status = {'status': 200, 'message': f'Created weight KPI for broiler ID: {broilerID}'}
            return jsonify(status)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/kpi/weight/get', methods=['POST'])
def kpi_weight_get():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                if 'kpi_weight_target' in doc:
                    return jsonify(doc['kpi_weight_target'])
                else:
                    status = {'status': 400, 'message': f'No weight KPI for broiler ID: {broilerID}'}
                    return jsonify(status), 400
            else:
                status = {'status': 400, 'message': f'No weight KPI for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': f'No weight KPI for broiler ID: {broilerID}'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/kpi/mortality/create', methods=['POST'])
def kpi_mortality_create():
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    green = data.get('green')
    if not green:
        status = {"status": 400, "message": "green missing"}
        return jsonify(status), 400

    red = data.get('red')
    if not red:
        status = {"status": 400, "message": "red missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                doc.update({
                    'kpi_mortality_target': dict(green=green, red=red)
                })
                db.save(doc)
                status = {'status': 200, 'message': f'Updated mortality KPI for broiler ID: {broilerID}'}
                return jsonify(status)
            else:
                db[str(broilerID)] = {
                    'kpi_mortality_target': dict(green=green, red=red)
                }
                status = {'status': 200, 'message': f'Created mortality KPI for broiler ID: {broilerID}'}
                return jsonify(status)
        else:
            server.create("health_targets")
            db = server['health_targets']
            db[str(broilerID)] = {
                'kpi_mortality_target': dict(green=green, red=red)
            }
            status = {'status': 200, 'message': f'Created mortality KPI for broiler ID: {broilerID}'}
            return jsonify(status),
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/kpi/mortality/get', methods=['POST'])
def kpi_mortality_get():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                if 'kpi_mortality_target' in doc:
                    d = doc['kpi_mortality_target']
                    return jsonify({
                        "green": float(d['green']), "amber": [float(d['green']), float(d['red'])], "red": float(d['red'])
                    })
                else:
                    status = {'status': 400, 'message': f'No mortality KPI for broiler ID: {broilerID}'}
                    return jsonify(status), 400
            else:
                status = {'status': 400, 'message': f'No mortality KPI for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': f'No mortality KPI for broiler ID: {broilerID}'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/kpi/fcr/create', methods=['POST'])
def kpi_fcr_create():
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    day0_21 = data.get('day0_21')
    if not day0_21:
        status = {"status": 400, "message": "day0_21 missing"}
        return jsonify(status), 400

    day22_35 = data.get('day22_35')
    if not day22_35:
        status = {"status": 400, "message": "day22_35 missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                doc.update({
                    'kpi_fcr': dict(day0_21=day0_21, day22_35=day22_35)
                })
                db.save(doc)
                status = {'status': 200, 'message': f'Updated FCR KPI for broiler ID: {broilerID}'}
                return jsonify(status)
            else:
                db[str(broilerID)] = {
                    'kpi_fcr': dict(day0_21=day0_21, day22_35=day22_35)
                }
                status = {'status': 200, 'message': f'Created FCR KPI for broiler ID: {broilerID}'}
                return jsonify(status)
        else:
            server.create("health_targets")
            db = server['health_targets']
            db[str(broilerID)] = {
                'kpi_fcr': dict(day0_21=day22_35, red=day22_35)
            }
            status = {'status': 200, 'message': f'Created FCR KPI for broiler ID: {broilerID}'}
            return jsonify(status),
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/kpi/fcr/get', methods=['POST'])
def kpi_fcr_get():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                if 'kpi_fcr' in doc:
                    return jsonify(doc['kpi_fcr'])
                else:
                    status = {'status': 400, 'message': f'No FCR KPI for broiler ID: {broilerID}'}
                    return jsonify(status), 400
            else:
                status = {'status': 400, 'message': f'No FCR KPI for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': f'No FCR KPI for broiler ID: {broilerID}'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/uniformity/sensor/create', methods=['POST'])
def uniformity_sensor_create():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    green = data.get('green')
    if not green:
        status = {"status": 400, "message": "green missing"}
        return jsonify(status), 400

    red = data.get('red')
    if not red:
        status = {"status": 400, "message": "red missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                doc.update({
                    'sensor_target': dict(green=green, red=red)
                })
                db.save(doc)
                status = {'status': 200, 'message': f'Updated sensor uniformity for broiler ID: {broilerID}'}
                return jsonify(status)
            else:
                db[str(broilerID)] = {
                    'sensor_target': dict(green=green, red=red)
                }
                status = {'status': 200, 'message': f'Created sensor uniformity for broiler ID: {broilerID}'}
                return jsonify(status)
        else:
            server.create("health_targets")
            db = server['health_targets']
            db[str(broilerID)] = {
                'sensor_target': dict(green=green, red=red)
            }
            status = {'status': 200, 'message': f'Created sensor uniformity for broiler ID: {broilerID}'}
            return jsonify(status)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/uniformity/sensor/get', methods=['POST'])
def uniformity_sensor_get():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                if 'sensor_target' in doc:
                    d = doc['sensor_target']
                    return jsonify({
                        "green": float(d['green']), "amber": [float(d['green']), float(d['red'])], "red": float(d['red'])
                    })
                else:
                    status = {'status': 400, 'message': f'No sensor target for broiler ID: {broilerID}'}
                    return jsonify(status), 400
            else:
                status = {'status': 400, 'message': f'No sensor target for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': f'No sensor target for broiler ID: {broilerID}'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/uniformity/luxsensor/create', methods=['POST'])
def uniformity_luxsensor_create():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    green = data.get('green')
    if not green:
        status = {"status": 400, "message": "green missing"}
        return jsonify(status), 400

    red = data.get('red')
    if not red:
        status = {"status": 400, "message": "red missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                doc.update({
                    'lux_sensor_target': dict(green=green, red=red)
                })
                db.save(doc)
                status = {'status': 200, 'message': f'Updated lux sensor uniformity for broiler ID: {broilerID}'}
                return jsonify(status)
            else:
                db[str(broilerID)] = {
                    'lux_sensor_target': dict(green=green, red=red)
                }
                status = {'status': 200, 'message': f'Created lux sensor uniformity for broiler ID: {broilerID}'}
                return jsonify(status)
        else:
            server.create("health_targets")
            db = server['health_targets']
            db[str(broilerID)] = {
                'lux_sensor_target': dict(green=green, red=red)
            }
            status = {'status': 200, 'message': f'Created lux sensor uniformity for broiler ID: {broilerID}'}
            return jsonify(status),
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/uniformity/luxsensor/get', methods=['POST'])
def uniformity_luxsensor_get():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                if 'lux_sensor_target' in doc:
                    d = doc['lux_sensor_target']
                    return jsonify({
                        "green": float(d['green']), "amber": [float(d['green']), float(d['red'])], "red": float(d['red'])
                    })
                else:
                    status = {'status': 400, 'message': f'No lux sensor target for broiler ID: {broilerID}'}
                    return jsonify(status), 400
            else:
                status = {'status': 400, 'message': f'No lux sensor target for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': f'No lux sensor target for broiler ID: {broilerID}'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/uniformity/weight/create', methods=['POST'])
def uniformity_weight_create():
    data = request.form.to_dict()
    broilerID = int(data.get('broilerID').strip())
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    green = data.get('green')
    if not green:
        status = {"status": 400, "message": "green missing"}
        return jsonify(status), 400

    red = data.get('red')
    if not red:
        status = {"status": 400, "message": "red missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                doc.update({
                    'uniformity_weight_target': dict(green=green, red=red)
                })
                db.save(doc)
                status = {'status': 200, 'message': f'Updated weight uniformity for broiler ID: {broilerID}'}
                return jsonify(status)
            else:
                db[str(broilerID)] = {
                    'uniformity_weight_target': dict(green=green, red=red)
                }
                status = {'status': 200, 'message': f'Created weight uniformity for broiler ID: {broilerID}'}
                return jsonify(status)
        else:
            server.create("health_targets")
            db = server['health_targets']
            db[str(broilerID)] = {
                'uniformity_weight_target': dict(green=green, red=red)
            }
            status = {'status': 200, 'message': f'Created weight uniformity for broiler ID: {broilerID}'}
            return jsonify(status), 200
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/health/targets/uniformity/weight/get', methods=['POST'])
def uniformity_weight_get():
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400

    try:
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in list(server):
            db = server['health_targets']
            if str(broilerID) in db:
                doc = db[str(broilerID)]
                if 'uniformity_weight_target' in doc:
                    d = doc['uniformity_weight_target']
                    return jsonify({
                        "green": float(d['green']), "amber": [float(d['green']), float(d['red'])], "red": float(d['red'])
                    })
                else:
                    status = {'status': 400, 'message': f'No weight uniformity target for broiler ID: {broilerID}'}
                    return jsonify(status), 400
            else:
                status = {'status': 400, 'message': f'No weight uniformity target for broiler ID: {broilerID}'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': f'No weight uniformity target for broiler ID: {broilerID}'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


class Targets:

    def __init__(self, data):
        self.data = data
        self.broilerID = self.data.get('broilerID')
        self.start_date = pd.to_datetime(data.get('start_date'), utc=True)
        self.end_date = pd.to_datetime(data.get('end_date'), utc=True)
        self.current_date = pd.to_datetime(data.get('current_date'), utc=True)
        self.base = f'{webapp}:178'
        # placement dates
        self.getPlacementWeights = requests.get(f'{self.base}/getPlacementWeights/{self.broilerID}')
        # broiler weights
        self.getBroilerWeights = requests.get(f'{self.base}/getBroilerWeights/{self.broilerID}')
        self.day = (self.current_date - self.start_date).days
        # KPI target for weight
        self.kpi_weight_target = {
            'day7': 4.6,
            'day14': 2.656,
            'day21': 2.021,
            'day28': 1.608,
            'day35': 11.8,
        }

        self.initialise()

    def initialise(self):
        server = connect_db(url=credentials['custom_url'])
        if "health_targets" in server:
            db = server['health_targets']
            if str(self.broilerID) in db:
                doc = db[str(self.broilerID)]
                temp = doc['kpi_weight_target']
                for k, v in temp.items():
                    temp[k] = float(v)
                self.kpi_weight_target = temp


    def weight(self):
        if self.getPlacementWeights.status_code != 200:
            return {"target_weight": [], "message": 'API is invalid: URL::' + self.getPlacementWeights.url}
        elif not self.getPlacementWeights.json():
            return {"kpi_weight": [], "message": 'API is not returning data: URL::' + self.getPlacementWeights.url}

        if self.getBroilerWeights.status_code != 200:
            return {"kpi_weight": [], "message": 'API is invalid: URL::' + self.getBroilerWeights.url}
        elif not self.getBroilerWeights.json():
            return {"kpi_weight": [], "message": 'API is not returning data: URL::' + self.getBroilerWeights.url}

        data_placement_df = pd.DataFrame(self.getPlacementWeights.json())
        if len(data_placement_df) == 0:
            status = {"target_weight": [],
                      "message": f"No weight data exist for {self.broilerID} using date "
                                 f"{self.current_date.isoformat()}"}
            return status
        data_placement_df['date'] = \
            data_placement_df['date'].apply(lambda a: pd.to_datetime(a, utc=True).date() if a else a)

        data_placement_df = data_placement_df.sort_values(by="date")

        # get placement date for a given start date
        temp_df = data_placement_df.loc[data_placement_df["date"] == self.start_date.date()]
        if len(temp_df) > 0:
            placement_weight = temp_df.iloc[-1]["totalBirdWeight"] / temp_df.iloc[-1]["noOfBirdsPerBox"]
        else:
            status = {
                "kpi_weight": [],
                "message": f"No placement weight data for given start date:: {self.start_date.date().isoformat()}"
            }
            return status
        if len(data_placement_df) == 0:
            status = {"kpi_weight": [],
                      "message": f"No weight data exist for {self.broilerID} using date "
                                 f"{self.current_date.isoformat()}"}
            return status
        # Get weight
        weight_df = pd.DataFrame(self.getBroilerWeights.json())
        if len(weight_df) == 0:
            status = {"kpi_weight": [],
                      "message": f"No broiler weights data exist for {self.broilerID} using date "
                                 f"{self.current_date.isoformat()}"}
            return status
        weight_df['dateTime'] = weight_df['dateTime'].apply(
            lambda a: pd.to_datetime(a, utc=True).date() if a else a)
        weight_df = weight_df.sort_values(by="dateTime")
        weight_df = weight_df.set_index('dateTime')
        weight_df = weight_df[['weightAverage']]

        indicator_day7 = lambda y: y-target_weight7 if y > target_weight7 else (0 if y == target_weight7 else 0)
        target_weight7 = placement_weight * self.kpi_weight_target['day7']

        if self.day <= 7:
            status = {'target_weight': target_weight7, 'day': float(7)}
        elif self.day <= 14:
            day = self.start_date.date()
            target_weight14 = self.kpi_weight_target['day14'] * weight_df.loc[day + timedelta(days=7)].tolist()[0]
            status = {'target_weight': float(target_weight14), "day": float(14)}
        elif self.day <= 21:
            day = self.start_date.date()
            target_weight21 = self.kpi_weight_target['day21'] * weight_df.loc[day + timedelta(days=14)].tolist()[0]
            status = {'target_weight': float(target_weight21), "day": float(21)}
        # day 28 target
        elif self.day <= 28:
            day = self.start_date.date()
            target_weight28 = self.kpi_weight_target['day28'] * weight_df.loc[day + timedelta(days=21)].tolist()[0]
            status = {'target_weight': float(target_weight28), "day": float(28)}
        # day 35 target
        else:
            day = self.start_date.date()
            day7_weight = weight_df.loc[day + timedelta(days=7)].tolist()[0]
            additional_weight = indicator_day7(day7_weight)

            target_weight35 = (target_weight7 * self.kpi_weight_target['day35']) + (additional_weight * 7)
            status = {'target_weight': float(target_weight35), "day": float(35)}

        return status


@app.route('/v1/api/health/targets/kpi/weight', methods=['POST'])
def target_weight():
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    if not data.get('start_date'):
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    if not data.get('end_date'):
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    if not data.get('current_date'):
        status = {"status": 400, "message": "current_date missing"}
        return jsonify(status), 400
    try:
        t = Targets(data)
        w = t.weight()
        return jsonify(w)
    except Exception as e:
        return {'status': 400, 'message': str(e)}
