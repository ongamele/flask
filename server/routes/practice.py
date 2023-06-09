import pandas as pd
import numpy as np
import pytz
import requests
import datetime as dt
from server import app, credentials, proxy, auth, webapp
from server.routes.index import connect_db
from flask import jsonify, request
from datetime import datetime
from frules.expressions import Expression as E
from frules.expressions import ltrapezoid, rtrapezoid, triangle
from frules.rules import Rule

# from apscheduler.schedulers.background import BackgroundScheduler
#
# scheduler = BackgroundScheduler(daemon=True)  # run jobs in background
# scheduler.start()  # start scheduler


def timezone_info(timezone):
    date_now = datetime.utcnow()
    date_now = pd.to_datetime(date_now.isoformat(), utc=True)
    date_now = date_now.tz_convert(pytz.FixedOffset(int(timezone) * 60))
    return {
        "timestamp_name": date_now.isoformat(),
        "timestamp": int(date_now.timestamp() * 1000),
    }


class Darkperiod:
    def __init__(self):
        self.iqrf_daily = "https://de7ece9a-7cfc-4e0c-8081-b24cbff85f51-bluemix:e7e8b60e9e2931c0ab23d016d3620192e5590" \
                          "7da8da44530e8d723cc4b141a34@de7ece9a-7cfc-4e0c-8081-b24cbff85f51-bluemix.cloudantnosqldb." \
                          "appdomain.cloud"
        self.couch = connect_db(self.iqrf_daily)
        self.current_date = None
        self.device_id = None
        self.timezone = None

    def duration(self, current_date, device_id, timezone):
        try:
            self.current_date = current_date
            self.device_id = device_id
            self.timezone = timezone
            db = self.couch[f'iotp_tk95td_default2_{self.current_date}']
            docs1 = db.view("iotp/by-date", include_docs=False)
            month = []
            try:
                for i in docs1:
                    t = i['value']
                    if 'data' in t:
                        if 'd' in i['value']['data']:
                            tt = i['value']['data']['d']
                            tt.update({'timestamp': i['value']['timestamp']})
                            month.append(tt)
                        elif 'data' in i['value']['data']:
                            tt = i['value']['data']['data']['d']
                            tt.update({'timestamp': i['value']['timestamp']})
                            month.append(tt)
                        else:
                            tt = i['value']['data']
                            tt.update({'timestamp': i['value']['timestamp']})
                            month.append(tt)

            except Exception:
                pass
            df1 = pd.DataFrame(month).dropna(subset=['Lux'])
            df1.timestamp = df1.timestamp.apply(pd.to_datetime)
            df1 = df1.loc[df1.ID == self.device_id]
            df1 = df1.sort_values(by='timestamp')
            if df1.iloc[0]['Lux'] == 0:  # if dark period started yesterday
                prev_day = (pd.to_datetime(self.current_date) - dt.timedelta(days=1)).date().isoformat()
                db = self.couch[f'iotp_tk95td_default2_{prev_day}']
                docs3 = db.view("iotp/by-date", include_docs=False)
                month = []
                try:
                    for i in docs3:
                        t = i['value']
                        if 'data' in t:
                            if 'd' in i['value']['data']:
                                tt = i['value']['data']['d']
                                tt.update({'timestamp': i['value']['timestamp']})
                                month.append(tt)
                            elif 'data' in i['value']['data']:
                                tt = i['value']['data']['data']['d']
                                tt.update({'timestamp': i['value']['timestamp']})
                                month.append(tt)
                            else:
                                tt = i['value']['data']
                                tt.update({'timestamp': i['value']['timestamp']})
                                month.append(tt)

                except Exception:
                    pass
                df3 = pd.DataFrame(month).dropna(subset=['Lux'])
                df3.timestamp = df3.timestamp.apply(pd.to_datetime)
                df3 = df3.loc[df3.ID == self.device_id]
                df3 = df3.sort_values(by='timestamp')
                if len(df3) > 0 and df3.iloc[-1]['Lux'] == 0:  # ignore dark period started yesterday
                    rows_idx = []
                    for i in df1.iterrows():
                        if i[1]['Lux'] == 0:
                            rows_idx.append(i[0])
                        else:
                            break
                    df1 = df1.drop(rows_idx)

            if df1.iloc[-1]['Lux'] == 0:  # if dark period ended next day
                next_day = (pd.to_datetime(self.current_date) + dt.timedelta(days=1)).date().isoformat()

                print(f'next day is: {next_day}')
                db = self.couch[f'iotp_tk95td_default2_{next_day}']
                docs2 = db.view("iotp/by-date", include_docs=False)
                month = []
                try:
                    for i in docs2:
                        t = i['value']
                        if 'data' in t:
                            if 'd' in i['value']['data']:
                                tt = i['value']['data']['d']
                                tt.update({'timestamp': i['value']['timestamp']})
                                month.append(tt)
                            elif 'data' in i['value']['data']:
                                tt = i['value']['data']['data']['d']
                                tt.update({'timestamp': i['value']['timestamp']})
                                month.append(tt)
                            else:
                                tt = i['value']['data']
                                tt.update({'timestamp': i['value']['timestamp']})
                                month.append(tt)

                except Exception:
                    pass
                df2 = pd.DataFrame(month).dropna(subset=['Lux'])
                df2.timestamp = df2.timestamp.apply(pd.to_datetime)
                df2 = df2.loc[df2.ID == self.device_id]
                df2 = df2.sort_values(by='timestamp')
                new_df = []
                for i in df2.iterrows():
                    if i[1]['Lux'] == 0:
                        new_df.append(i[1])
                    else:
                        new_df.append(i[1])
                        break
                new_df = pd.DataFrame(new_df)
                df1 = pd.concat([df1, new_df], ignore_index=True)
                # checkpoint

            df1.timestamp = df1.timestamp.apply(lambda x: pd.to_datetime(x)
                                                .tz_convert(pytz.FixedOffset(self.timezone*60))) #.replace(tzinfo=None))

            db = {}
            final_docs = []
            for i in df1.iterrows():
                lux = i[1]['Lux']
                device_id = i[1]['ID']

                if lux == 0:
                    if 'compute-dark-period' in db:
                        doc = db['compute-dark-period']
                        if device_id in doc:
                            current_time = i[1]['timestamp'].isoformat()
                            duration = round((pd.to_datetime(current_time) - pd.to_datetime(
                                doc[device_id]['start_time'])).total_seconds() / (60 * 60), 1)
                            doc[device_id].update({
                                'duration': duration,
                            })
                            db.update(doc)
                            print(f'Updated {device_id} dark period duration')
                        else:
                            start_time = i[1]['timestamp'].isoformat()
                            duration = 0

                            doc[device_id] = {
                                'start_time': start_time,
                                'duration': duration,
                                'end_time': 'pending'
                            }
                            db.update(doc)
                            print(f'Device {device_id} started dark period')
                    else:
                        start_time = i[1]['timestamp'].isoformat()
                        duration = 0
                        db['compute-dark-period'] = {
                            device_id: {
                                'start_time': start_time,
                                'duration': duration,
                                'end_time': 'pending'
                            }
                        }
                        print(f'Device {device_id} added to dark period')
                elif lux > 0:
                    if 'compute-dark-period' in db:
                        doc = db['compute-dark-period']
                        if device_id in doc:
                            if doc[device_id]['end_time'] == 'pending':
                                end_time = i[1]['timestamp'].isoformat()
                                duration = round((pd.to_datetime(end_time) - pd.to_datetime(
                                    doc[device_id]['start_time'])).total_seconds() / (60 * 60), 1)
                                d = {
                                    'day': pd.to_datetime(doc[device_id]['start_time']).date().isoformat(),
                                    'device_id': device_id,
                                    'start_time': doc[device_id]['start_time'],
                                    'duration': duration,
                                    'end_time': end_time
                                }
                                doc.pop(device_id)  # remove device id
                                final_docs.append(d)
                                print('Appended duration')
            final_docs_df = pd.DataFrame(final_docs)
            final_docs_df = final_docs_df.sort_values(by='duration')
            df = final_docs_df.loc[final_docs_df.day == self.current_date]
            duration = df.loc[df.device_id == self.device_id, 'duration'].sum()
            try:
                dates = [i[1].to_dict() for i in df[['start_time', 'duration', 'end_time']].iterrows()]
            except Exception:
                dates = []
            return duration, dates
        except Exception as e:
            return 0, []


class Practice:

    def __init__(self):
        self.base = f'{webapp}:178'
        self.params = None
        self.strain_file = None
        self.strain_id = None
        self.parent_flock_age = None
        self.credentials = credentials
        self.day = None
        self.water_quality = "water quality"
        self.air_quality = "air quality"
        self.floor_temperature = "floor temperature"
        self.get_device_details = None
        self.broilerID = None
        self.cycles = None
        self.cycle_id = None
        self.auth = auth
        self.catch_date = None
        self.start_date = None
        self.end_date = None
        self.current_date = None
        self.timezone = None

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

    def equality_rule(self, rules, day, category, sensor, reading):
        """

        Args:
            rules: strain file
            day: cycle day
            category: air or water quality
            sensor:
            reading: sensor data

        Returns:

        """
        try:
            if sensor == "Lux" and self.catch_date and self.day in [34, 35]:
                t = 'daylight' if self.current_date <= self.catch_date and (6 < self.catch_date.hour < 18) else 'night'
                equality = rules[day][category][sensor][t]["equality"]
                value = rules[day][category][sensor][t]["value"]
            else:
                equality = rules[day][category][sensor]["equality"]
                value = rules[day][category][sensor]["value"]
        except Exception as e:
            status = {'status': 400, 'message': "Error: " + str(e)}
            raise Exception(str(status))

        if equality == "<":
            upper_bound = value
            fuzzy_upper = value * (1 - 0.1)

            truth = E(ltrapezoid(value * (1 - 0.1), value), "truth")
            is_truth = Rule(truth=truth)
            fuzzy_upper_value = is_truth.eval(truth=reading)

            return {"fuzzy_upper_bounds": {"upper_bound": upper_bound, "fuzzy_upper": fuzzy_upper,
                                           "fuzzy_upper_value": fuzzy_upper_value}}, [0, value], reading

        if equality == "=<":
            upper_bound = value
            fuzzy_upper = value * (1 - 0.1)

            truth = E(ltrapezoid(value * (1 - 0.1), value + 10 ** (-5)), "truth")
            is_truth = Rule(truth=truth)
            fuzzy_upper_value = is_truth.eval(truth=reading)

            return {"fuzzy_upper_bounds": {"upper_bound": upper_bound, "fuzzy_upper": fuzzy_upper,
                                           "fuzzy_upper_value": fuzzy_upper_value}}, [0, value], reading

        if equality == ">":
            lower_bound = value[0]
            fuzzy_lower = value[0] * (1 + 0.1)

            truth = E(rtrapezoid(value, value * (1 + 0.1)), "truth")
            is_truth = Rule(truth=truth)
            fuzzy_lower_value = is_truth.eval(truth=reading)

            return {"fuzzy_lower_bounds": {"lower_bound": lower_bound, "fuzzy_lower": fuzzy_lower,
                                           "fuzzy_lower_value": fuzzy_lower_value}}, [value, "Infinity"], reading

        if equality == ">=":
            lower_bound = value
            fuzzy_lower = value * (1 + 0.1)

            truth = E(rtrapezoid(value - 10 ** (-5), value * (1 + 0.1)), "truth")
            is_truth = Rule(truth=truth)
            fuzzy_lower_value = is_truth.eval(truth=reading)

            return {"fuzzy_lower_bounds": {"lower_bound": lower_bound, "fuzzy_lower": fuzzy_lower,
                                           "fuzzy_lower_value": fuzzy_lower_value}}, [value, "Infinity"], reading

        if equality == "interval":

            lower_bound = value[0]
            fuzzy_lower = value[0] * (1 + 0.1)

            upper_bound = value[1]
            fuzzy_upper = value[1] * (1 - 0.1)

            # ">="
            truth = E(rtrapezoid(value[0] - 10 ** (-5), value[0] * (1 + 0.1)), "truth")
            is_truth = Rule(truth=truth)
            fuzzy_lower_value = is_truth.eval(truth=reading)

            # "=<"
            truth = E(ltrapezoid(value[1] * (1 - 0.1), value[1] + 10 ** (-5)), "truth")
            is_truth = Rule(truth=truth)
            fuzzy_upper_value = is_truth.eval(truth=reading)

            if fuzzy_upper_value == 1.0:
                fuzzy_upper_value = None

            if fuzzy_lower_value == 1.0:
                fuzzy_lower_value = None

            return {"fuzzy_lower_bounds": {"lower_bound": lower_bound, "fuzzy_lower": fuzzy_lower,
                                           "fuzzy_lower_value": fuzzy_lower_value},
                    "fuzzy_upper_bounds": {"upper_bound": upper_bound, "fuzzy_upper": fuzzy_upper,
                                           "fuzzy_upper_value": fuzzy_upper_value}
                    }, value, reading

        if equality == "=":
            if value == 0:
                if value == reading:
                    return 1.0, value, reading
                if value != reading:
                    return 0, value, reading
            truth = E(triangle(value * (1 - 0.005), value * (1 + 0.005)), "truth")
            is_truth = Rule(truth=truth)
            return is_truth.eval(truth=reading), value, reading

        if equality == "list":

            if self.parent_flock_age is not None and self.parent_flock_age > 30:
                value = min(value)
                truth = E(triangle(value * (1 - 0.005), value * (1 + 0.005)), "truth")
                is_truth = Rule(truth=truth)
                return is_truth.eval(truth=reading), value, reading
            else:
                value = max(value)
                truth = E(triangle(value * (1 - 0.005), value * (1 + 0.005)), "truth")
                is_truth = Rule(truth=truth)
                return is_truth.eval(truth=reading), value, reading

    def category(self, rules, category_, reading):
        """
        Args:
            rules: strain file
            category_: air or water quality
            reading: sensor data
        Returns:
        """

        aq_vars = rules["day " + str(self.day)][category_].keys()
        aq_vars = list(aq_vars)

        reading_vars = reading.keys()
        reading_vars = list(reading_vars)

        results_dict = {}
        results_value = {}
        actual_values = {}

        for var in aq_vars:

            if var in reading_vars:
                results_dict[var], results_value[var], actual_values[var] = self.equality_rule(rules,
                                                                                               "day " + str(self.day),
                                                                                               category_,
                                                                                               var, reading[var])
        return results_dict, results_value, actual_values

    def log_error(self, status, typ):
        """Log errors to cloudant DB

        Args:
            status: message
            typ: which model type

        Returns:
            None
        """
        server = connect_db(url=self.credentials['custom_url'])
        temp = {
            "status": status,
            "timestamp_name": datetime.utcnow().isoformat(),
            "timestamp": int(datetime.utcnow().timestamp()*1000)
        }
        if typ == "best-practice":
            if "error-logs" in list(server):
                db = server['error-logs']
                if "best-practice-model" in db:
                    doc = db["best-practice-model"]
                    doc.update(temp)
            else:
                server.create("error-logs")
                db = server['error-logs']
                db["best-practice-model"] = temp

    def get_strain_file(self):
        """
        Get stain file
        Args:

        Returns:
            strain file

        """
        strain_server = connect_db(url=self.credentials['custom_url'])  # establish DB connection

        strain_table = strain_server[credentials['strain']]
        if self.strain_id in list(strain_table):
            self.strain_file = strain_table[self.strain_id]['file']
            return self.strain_file
        else:
            status = "Best practice model cannot find strain ID"
            # todo

    def devices(self):
        self.get_device_details = requests.get(f'{self.base}/getDeviceDetails', proxies=proxy, auth=self.auth)
        return self.get_device_details

    def get_broiler_name(self, broiler_id):
        h = requests.get(f'{self.base}/getAllBroilers', auth=self.auth)
        if h.status_code == 200:
            for i in h.json():
                id_ = i.get('id')
                if id_ == int(broiler_id):
                    return i['name']

    def get_cycles(self):
        cycles = requests.get(f'{self.base}/getCylces', proxies=proxy, auth=self.auth)
        if cycles.status_code == 200:
            df = pd.DataFrame(cycles.json())
            df = df.sort_values(by='id')
            x = df.loc[df.broilerid == int(self.broilerID), ['name', 'id']]
            name = x.iloc[-1]['name']
            self.cycle_id = x.iloc[-1]['id']
            self.cycles = name

    def get_catch_data(self):
        catch = requests.get(f'{self.base}/getBroilerSettings/{self.broilerID}', proxies=proxy, auth=self.auth)
        if catch.status_code == 200:
            df = pd.DataFrame(catch.json())
            if len(df) > 0:
                df['dateFinalCatch'] = df['dateFinalCatch'].apply(lambda x: pd.to_datetime(x, utc=True))
                df.dropna(axis=0, how='any', subset=['placementDate', 'dateFinalCatch'], inplace=True)
                self.catch_date = df.sort_values(by='dateFinalCatch').iloc[-1]['dateFinalCatch']

    def get_latest(self, data):

        broilerID = data.get('broilerID')
        self.broilerID = broilerID
        self.strain_id = data.get('strain_id')
        self.timezone = data.get('timezone', 2)  # set timezone, default is 2
        self.timezone = int(self.timezone)
        self.parent_flock_age = data.get('parent_flock_age')
        self.start_date = pd.to_datetime(data.get('start_date'), utc=True)
        self.end_date = pd.to_datetime(data.get('end_date'), utc=True)
        self.current_date = pd.to_datetime(data.get('current_date'), utc=True)

        if not broilerID:
            status = {"status": 400, "message": "broilerID missing"}
            return jsonify(status), 400
        if not self.strain_id:
            status = {"status": 400, "message": "strain_id missing"}
            return jsonify(status), 400
        if not self.start_date:
            status = {"status": 400, "message": "start_date missing"}
            return jsonify(status), 400
        if not self.end_date:
            status = {"status": 400, "message": "end_date missing"}
            return jsonify(status), 400
        if not self.current_date:
            status = {"status": 400, "message": "current_date missing"}
            return jsonify(status), 400
        if not self.timezone:
            status = {"status": 400, "message": "timezone missing"}
            return jsonify(status), 400
        if not self.parent_flock_age:
            status = {"status": 400, "message": "parent_flock_age missing"}
            return jsonify(status), 400
        self.parent_flock_age = float(self.parent_flock_age)
        strain = self.get_strain_file()
        # find  day
        if self.start_date > self.current_date:
            self.day = -1
        else:
            self.day = (self.current_date - self.start_date).days

        self.day = min(self.day, len(strain.keys())-2)

        # Get cycles
        self.get_cycles()
        # Get device types
        self.devices()
        if self.get_device_details.status_code != 200:
            status = {"status": 400, "message": "Could not getDeviceDetails"}
            return jsonify(status), 400
        check_device = pd.DataFrame(self.get_device_details.json())[['deviceID', 'deviceType']]

        # get catch date
        self.get_catch_data()

        # Get devices
        k = requests.get(f'{self.base}/getBriolerDetails/{broilerID}')
        if k.status_code != 200:
            status = {"status": 400, "message": "Could not getBriolerDetails"}
            return jsonify(status), 400
        else:
            broiler_devices = set()
            for i in k.json()['tbl_Devices']:
                for j in i['tbl_DeviceDataDetail']:
                    broiler_devices.add(j['deviceID'])

        k = {i: requests.get(f'{self.base}/getLatestRaw/{i}') for i in broiler_devices}
        for i in k:
            if k[i].status_code != 200:
                status = {"status": 400, "message": "Could not get getLatestRaw/" + i}
                return status
        for j in k:
            temp = k[j].json()
            if 'd' in temp:
                temp = temp['d']
                if 'ID' in temp:
                    temp.pop('ID')
                if 'BAT' in temp:
                    temp.pop('BAT')
                if 'EC' in temp:
                    temp.pop('EC')
                k[j] = temp
            else:
                status = {"status": 400, "message": "Could not get sensor payload of ID="+j}
                return status

        results = {}
        for key in k:
            device_type = check_device.loc[check_device.deviceID == key, "deviceType"].values.tolist()
            if device_type:
                device_type = device_type[0]
            if "SW" in device_type and self.day != -1:
                water = k[key]

                optimal_w, optimal_w_value, actual_values = self.category(strain, self.water_quality, water)

                results[key] = {
                    "indicators": optimal_w,
                    "best_practice_limits": optimal_w_value,
                    "actual_values": actual_values,
                }

            elif "SC" in device_type:
                air = k[key]
                if 'Lux' in air:
                    dark = Darkperiod()
                    air['Dark period'], dates = dark.duration(current_date=self.current_date.date().isoformat(),
                                                              device_id=key, timezone=self.timezone)
                optimal_w, optimal_w_value, actual_values = self.category(strain, self.air_quality, air)
                if 'Dark period' in actual_values:
                    value = actual_values.pop('Dark period')
                    actual_values['Dark period'] = {'value': value, 'dates': dates}
                if optimal_w or optimal_w_value or actual_values:
                    results[key] = {
                        "indicators": optimal_w,
                        "best_practice_limits": optimal_w_value,
                        "actual_values": actual_values,
                    }
        results.update(self.broiler_info())
        return results

    def get_deviations_env(self, data):
        d = self.get_latest(data)
        results = {}
        for i in d:
            if i in ['broiler_info', 'timestamp_name', 'timestamp']:
                continue
            actual_values = d[i]['actual_values']
            indicators = d[i]['indicators']
            best_practice_limits = d[i]['best_practice_limits']
            temp = {}
            for j in actual_values:
                sensor = actual_values[j]
                target = best_practice_limits[j]
                deviation = 0
                deviationp = 0
                if j == 'Temp' and isinstance(indicators[j], (float, int)):
                    deviation = abs(sensor - target)
                    deviationp = round(abs(sensor - target) / target * 100, 2)
                elif j == 'Dark period':
                    if target == 0:
                        deviation = sensor['value']
                        deviationp = 100
                        if sensor['value'] == 0:
                            deviationp = 0
                    else:
                        deviation = abs(sensor['value'] - target)
                        deviationp = round(abs(sensor['value'] - target) / target * 100, 2)
                elif j == 'Lux' and type(target) != list:
                    if target == 0:
                        deviation = sensor
                        deviationp = 100
                        if sensor == 0:
                            deviationp = 0
                    else:
                        deviation = abs(sensor - target)
                        deviationp = round(abs(sensor - target) / target * 100, 2)
                else:
                    if 'fuzzy_upper_bounds' in indicators[j] and sensor > max(target):
                        deviation = abs(sensor - max(target))
                        deviationp = round(abs(sensor - max(target)) / max(target) * 100, 2)
                    elif 'fuzzy_lower_bounds' in indicators[j]:
                        if "Infinity" in target:
                            target[target.index("Infinity")] = np.inf
                        if sensor < min(target):
                            deviation = abs(sensor - min(target))
                            deviationp = round(abs(sensor - min(target)) / min(target) * 100, 2)
                        if np.inf in target:
                            target[target.index(np.inf)] = "Infinity"
                data_dict = {"sensor": sensor,
                             "target": target,
                             "deviation": deviation,
                             "deviationP": deviationp}
                temp[j] = data_dict
            results[i] = temp
            results['broiler_info'] = d['broiler_info']
            results['timestamp_name'] = d['timestamp_name']
            results['timestamp'] = d['timestamp']
        return results


class FCR:

    def __init__(self, broilerID, noChickens, start_date, end_date, current_date, cycle_id):

        self.broilerID = broilerID
        self.noChickens = noChickens
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = current_date
        self.cycle_id = cycle_id

        self.mortality_df = None
        self.feeds_df = None
        self.growths_df = None
        self.placement_weight = None
        self.catch_total_weight = None  # at end of the cycle
        self.catch_no_of_birds = None  # at end of the cycle

    def broiler_info(self):
        d = {
            'broiler_info': {
                'broiler_ID': str(self.broilerID),
                'start_date': self.start_date,
                'current_date': self.current_date,
                'end_date': self.end_date,
                'broiler_cycle_day': (pd.to_datetime(self.current_date).date() - pd.to_datetime(self.start_date).date()).days,
                'cycle_id': str(self.cycle_id),
            }
        }
        return d

    def _get_current_mortalities_df(self):

        api_call = requests.get(f'{webapp}:178/getBriolerDetails/{self.broilerID}',
                                proxies={"http": None, "https": None, })

        mortality_data = []

        for i in api_call.json()['tbl_BriolerData']:
            mortality_data.append({"broilerID": i['briolerID'], "fatalities": i['fatalities'], "culls": i['culls'],
                                   "date": i['dateTime']})

        mortality_df = pd.DataFrame(mortality_data)

        mortality_df["date"] = mortality_df["date"].apply(lambda x: pd.to_datetime(x).date())
        mortality_df = mortality_df.sort_values(by=["date"])

        # Mort = fatalities + culls
        mortality_df["MORT"] = mortality_df["fatalities"] + mortality_df["culls"]

        self.mortality_df = mortality_df

        return self

    def _get_current_feeds_df(self):

        feeds_data = requests.get(f'{webapp}:178/getWaterFeedData/{self.broilerID}',
                                  proxies={"http": None, "https": None, }).json()
        feeds_df = pd.DataFrame(feeds_data)

        feeds_df["dateTime"] = feeds_df["dateTime"].apply(lambda x: pd.to_datetime(x).date())

        self.feeds_df = feeds_df

        return self

    def _get_growths_df(self):

        growths_data = requests.post(f'{webapp}:178/getHistoricalWeight/',
                                     proxies={"http": None, "https": None, },
                                     json={"id": self.broilerID, "start": self.start_date,
                                           "end": self.current_date + "T23:59:59"}).json()
        growths_df = pd.DataFrame(growths_data)
        try:
            growths_df["dateTime"] = growths_df["dateTime"].apply(lambda x: pd.to_datetime(x).date())
        except Exception:
            raise Exception(f'Consumption data is missing for current date {self.current_date}')
        self.growths_df = growths_df

        return self

    def _get_placement_weight(self):

        data_placement = requests.get(f'{webapp}:178/getPlacementWeights/' + str(self.broilerID),
                                      proxies={"http": None, "https": None, }).json()
        data_placement_df = pd.DataFrame(data_placement)
        data_placement_df['date'] = pd.to_datetime(data_placement_df['date'])
        data_placement_df = data_placement_df.sort_values(by="date")
        data_placement_df = data_placement_df.loc[data_placement_df["date"] <= self.current_date]
        placement_weight = data_placement_df.iloc[-1]["totalBirdWeight"] / data_placement_df.iloc[-1]["noOfBirdsPerBox"]

        self.placement_weight = placement_weight

        return self

    def _get_catch_data(self):

        catch_data = requests.get(f'{webapp}:178/getCatchData/{self.broilerID}',
                                  proxies={"http": None, "https": None, }).json()
        catch_data_df = pd.DataFrame(catch_data)
        catch_data_df["date"] = catch_data_df["date"].apply(lambda x: pd.to_datetime(x).date())
        catch_data_df["date"] = pd.to_datetime(catch_data_df['date'])

        self.catch_total_weight = catch_data_df[catch_data_df["date"] == self.end_date]["totalweight"].values[0]
        self.catch_no_of_birds = catch_data_df[catch_data_df["date"] == self.end_date]["noofbirds"].values[0]

        return self

    def compute_FCRs(self):
        """
        FCR = (Average feed consumed per bird for the week) / (Average weight gain per bird)

            where:
             “Average feed consumed per bird for the week” =
            (total feed consumed for the week) / (total number of live birds at end of week)

            “Average weight gain per bird” =
            (average bird weight at end of week) – (average bird weight at start of week)
        """

        # Get all data
        self._get_current_feeds_df()
        self._get_current_mortalities_df()
        self._get_growths_df()
        self._get_placement_weight()

        # get number of FCRs to compute, ie number of weights recorded between start and current date
        no_days = (pd.to_datetime(self.current_date).date() - pd.to_datetime(self.start_date).date()).days
        no_fcrs = no_days // 7

        # get number of remaining days between catch date and last recorded weights
        no_days_start_to_end = (pd.to_datetime(self.end_date).date() - pd.to_datetime(self.start_date).date()).days
        no_day_rem = no_days_start_to_end % 7

        # if end date is the current date, get the catch data
        if self.end_date == self.current_date:

            try:
                self._get_catch_data()
            except:
                return {"status": 400,
                        "message": "Either the end_date is wrong or the catch data for the end of the cycle has not been recorded"}

            if no_day_rem == 0:
                no_fcrs = no_fcrs - 1

        fcr_list = []

        if no_fcrs > 0:

            try:
                for day in range(0, 7 * no_fcrs, 7):  # go through all FCRs needed to be calculated

                    if day == 0:  # calculate FCR from day 0 to day 7, ie, 8 days are considered

                        # collect data from data frames from day 0 to day 7, ie return data between day 0 and day 7, day 7 inclusive

                        feeds = self.feeds_df[(self.feeds_df["dateTime"] >= (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day)).date())
                                              & (self.feeds_df["dateTime"] <= (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7)).date())]

                        mortalities = self.mortality_df[
                            (self.mortality_df["date"] >= (pd.to_datetime(self.start_date)).date())
                            & (self.mortality_df["date"] <= (
                                        pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7)).date())]

                        avg_feed_cons = feeds["feed"].sum() / (self.noChickens - mortalities["MORT"].sum())

                        avg_weight_gain = self.growths_df[self.growths_df["dateTime"] == (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7))][
                                              "weightAverage"].values[0] \
                                          - self.placement_weight

                    else:  # calculate FCRs over 7 day period after day 7, ie 7 days considered

                        feeds = self.feeds_df[(self.feeds_df["dateTime"] > (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day)).date())
                                              & (self.feeds_df["dateTime"] <= (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7)).date())]

                        mortalities = self.mortality_df[
                            (self.mortality_df["date"] > (pd.to_datetime(self.start_date)).date())
                            & (self.mortality_df["date"] <= (
                                        pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7)).date())]

                        avg_feed_cons = feeds["feed"].sum() / (self.noChickens - mortalities["MORT"].sum())

                        avg_weight_gain = self.growths_df[self.growths_df["dateTime"] == (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7))][
                                              "weightAverage"].values[0] \
                                          - self.growths_df[self.growths_df["dateTime"] == (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day))]["weightAverage"].values[
                                              0]

                        _trailing_weight = self.growths_df[self.growths_df["dateTime"] == (
                                    pd.to_datetime(self.start_date) + dt.timedelta(days=day + 7))][
                            "weightAverage"].values[0]

                    fcr = avg_feed_cons / avg_weight_gain
                    fcr_list.append(fcr)

            except:
                return {"status": 400,
                        "message": "Please check if weights, placement, feed and mortalites data has been recorded between start and current date of the cycle"}

            if self.end_date == self.current_date:

                if no_day_rem == 0:  # ie if the end date is on day 28,35,42, ie a day divisible by 7

                    feeds = self.feeds_df[
                        (self.feeds_df["dateTime"] > (pd.to_datetime(self.end_date) - dt.timedelta(days=7)).date())
                        & (self.feeds_df["dateTime"] <= (pd.to_datetime(self.end_date)).date())]

                    avg_feed_cons = feeds["feed"].sum() / self.catch_no_of_birds

                    avg_weight_gain = (self.catch_total_weight / self.catch_no_of_birds) \
                                      - self.growths_df[self.growths_df["dateTime"] == (
                                pd.to_datetime(self.end_date) - dt.timedelta(days=7))]["weightAverage"].values[0]

                    fcr = avg_feed_cons / avg_weight_gain
                    fcr_list.append(fcr)

                else:  # ie if the end date is on day eg 36,38,45, ie a day not divisible by 7

                    feeds = self.feeds_df[(self.feeds_df["dateTime"] > (
                                pd.to_datetime(self.end_date) - dt.timedelta(days=no_day_rem)).date())
                                          & (self.feeds_df["dateTime"] <= (pd.to_datetime(self.end_date)).date())]

                    avg_feed_cons = feeds["feed"].sum() / self.catch_no_of_birds

                    avg_weight_gain = (self.catch_total_weight / self.catch_no_of_birds) - _trailing_weight
                    fcr_end_date = avg_feed_cons / avg_weight_gain

            output_dict = {"FCRs": {}}

            i = 7
            for value, fcr in enumerate(fcr_list):
                output_dict["FCRs"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=i)).date())] = {}
                output_dict["FCRs"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=i)).date())][
                    "broiler_cycle_day"] = i

                output_dict["FCRs"][str((pd.to_datetime(self.start_date) + dt.timedelta(days=i)).date())][
                    "FCR value"] = fcr

                i = i + 7

            if (self.end_date == self.current_date) and (no_day_rem > 0):
                output_dict["FCRs"][str(pd.to_datetime(self.end_date).date())] = {}
                output_dict["FCRs"][str(pd.to_datetime(self.end_date).date())]["broiler_cycle_day"] = (
                            pd.to_datetime(self.end_date).date() - pd.to_datetime(self.start_date).date()).days

                output_dict["FCRs"][str(pd.to_datetime(self.end_date).date())]["FCR value"] = fcr_end_date

            return output_dict
        else:
            return {"status": 400,
                    "message": "Missing feeds in the SQL database or the first seven days in "
                               "the broiler cycle have not elapsed"}


@app.route('/v1/api/practice/latest', methods=['POST'])
def api_latest():
    """Get latest optimal values

    tags:
      - API
    responses:
    """
    practice = Practice()
    try:
        data = request.form.to_dict()
        results = practice.get_latest(data)
        return results
    except Exception as e:
        status = {"status": 400, "message": str(e)}
        # practice.log_error(status=status, typ="best-practice")  # todo
        return status


@app.route('/v1/api/practice', methods=['POST'])
def api_practice():
    practice = Practice()
    try:
        data = request.form.to_dict()

        broilerID = data.get('broilerID')
        if not broilerID:
            status = {"status": 400, "message": "broilerID missing"}
            return jsonify(status), 400

        strain_id = data.get('strain_id')
        if not strain_id:
            status = {"status": 400, "message": "strain_id missing"}
            return jsonify(status), 400

        start_date = pd.to_datetime(data.get('start_date'), utc=True)
        end_date = pd.to_datetime(data.get('end_date'), utc=True)
        current_date = pd.to_datetime(data.get('current_date'), utc=True)

        if not start_date:
            status = {"status": 400, "message": "start_date missing"}
            return jsonify(status), 400

        if not end_date:
            status = {"status": 400, "message": "end_date missing"}
            return jsonify(status), 400

        if not current_date:
            status = {"status": 400, "message": "current_date missing"}
            return jsonify(status), 400

        practice.strain_id = strain_id
        strain = practice.get_strain_file()
        if start_date > current_date:
            day = -1
        else:
            day = (current_date - start_date).days

        day = min(day, len(strain.keys())-2)

        day_practice = {
            'broiler_info': {
                'broiler_ID': broilerID,
                'start_date': start_date.date().isoformat(),
                'current_date': current_date.date().isoformat(),
                'end_date': end_date.date().isoformat(),
                'broiler_cycle_day': day},
            'best_practice': strain[f"day {day}"]
        }
        return jsonify(day_practice)
    except Exception as e:
        status = {"status": 400, "message": str(e)}
        # practice.log_error(status=status, typ="best-practice")  # todo
        return jsonify(status), 400


@app.route("/v1/api/practice/fcr", methods=['POST'])
def fcr():
    data = request.form.to_dict()

    broilerID = int(data.get('broilerID'))
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    current_date = data.get('current_date')
    cycle_id = data.get('cycle_id')
    timezone = data.get('timezone', 2)
    noChickens = int(data.get('noChickens'))

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

    if not noChickens:
        status = {"status": 400, "message": "noChickens missing"}
        return status
    FCR_model = FCR(broilerID=broilerID, noChickens=noChickens, start_date=start_date, end_date=end_date,
                    current_date=current_date, cycle_id=cycle_id)
    try:
        results = FCR_model.compute_FCRs()
        results.update(FCR_model.broiler_info())
        results.update(timezone_info(timezone=timezone))

    except Exception as e:
        results = {'status': 400, 'message': "FCR error: " + str(e)}
        results.update(FCR_model.broiler_info())
        results.update(timezone_info(timezone=timezone))
    return jsonify(results)
