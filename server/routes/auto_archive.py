import pytz
from flask import jsonify

from server import app, credentials, auth, webapp
from server.routes.health_score import HealthScore
from server.routes.practice import connect_db, FCR, timezone_info
from server.routes.practice import Practice
import pandas as pd
import requests
import datetime
from os.path import dirname, abspath, exists
from os import remove
from server.routes.predictive import GrowthPredictions, MortalityPredictions
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler(daemon=True)  # run jobs in background
scheduler.start()  # start scheduler


class Archive:
    def __init__(self):
        self.timezone = 2  # todo: get timezone from webapp
        self.process_data = {}
        self.process_status = {}
        self.option = None

    def runner(self, option):
        self.option = option
        req = requests.get(f'{webapp}:178/getAllBroilers')
        broilerIDs = pd.DataFrame(req.json())['id']
        for broilerID in broilerIDs.values:
            messages = []
            req = requests.get(f'{webapp}:178/getBroilerSettings/{broilerID}')
            df = pd.DataFrame(req.json())
            if len(df) == 0:
                continue
            df = df.sort_values(by='dateFinalCatch')
            df.dropna(axis=0, how='any', subset=['placementDate', 'dateFinalCatch'], inplace=True)
            if len(df) == 0:
                continue
            start_date = df.iloc[-1]['placementDate']
            if not start_date:
                messages.append({'start_date': {'status': 400, 'message': 'Could not obtain start date'}})
            end_date = df.iloc[-1]['dateFinalCatch']
            if not end_date:
                messages.append({'end_date': {'status': 400, 'message': 'Could not obtain end date'}})
            strain_id = df.iloc[-1]['strain']
            if not strain_id:
                messages.append({'strain_id': {'status': 400, 'message': 'Could not obtain strain id'}})
            else:
                strain_id = str(strain_id)

            cycle_id = df.iloc[-1]['cycleId']
            if not cycle_id:
                messages.append({'cycle_id': {'status': 400, 'message': 'Could not obtain cycle ID'}})
            req = requests.get(f'{webapp}:178/getPlacementWeights/{broilerID}')
            noChickens = pd.DataFrame(req.json())
            if len(noChickens) != 0:
                noChickens['date'] = noChickens['date'].apply(lambda x: pd.to_datetime(x).date())
                noChickens = noChickens.loc[noChickens['date'] == pd.to_datetime(start_date).date()]
                if len(noChickens) != 0:
                    noChickens = noChickens.iloc[-1]['noOfBirdsPerBox']
                else:
                    noChickens = 38000  # todo: noChickens default value. An API will be created by Michael to get this value before placement
            else:
                noChickens = 38000  # todo: noChickens default value. An API will be created by Michael to get this value before placement
            flock = requests.get(f'{webapp}:178/getDOCData/{broilerID}').json()
            flock = pd.DataFrame(flock)
            flock = flock[flock.cycleId == cycle_id]
            try:
                flock = min(flock['parentAge'].values)
            except:
                messages.append({'parent_flock_age': {'status': 400, 'message': 'Could not obtain parent flock age'}})

            s = pd.to_datetime(start_date, utc=True) - datetime.timedelta(1)  # todo .tz_convert(pytz.FixedOffset(self.timezone*60))  # start
            e = pd.to_datetime(end_date, utc=True)  # todo .tz_convert(pytz.FixedOffset(self.timezone*60))  # end
            now = datetime.datetime.now(tz=pytz.FixedOffset(self.timezone*60))
            if s < now < e:
                self.process_status[f'{broilerID}'] = {'status': 'Inactive',
                                                       'message': f'There is no active cycle for broiler {broilerID}.'
                                                                  f' See details for missing information',
                                                       'details': messages}
                if len(messages) > 0:  # if there are errors continue
                    continue
                self.process_data[f'{broilerID}'] = dict(broilerID=str(broilerID),
                                                         start_date=pd.to_datetime(start_date).date().isoformat(),
                                                         end_date=pd.to_datetime(end_date).date().isoformat(),
                                                         current_date=now.date().isoformat(),
                                                         cycle_id=str(int(cycle_id)),
                                                         noChickens=int(noChickens),
                                                         parent_flock_age=int(flock),
                                                         strain_id=strain_id,
                                                         timezone=self.timezone,
                                                         n_days=3)
                self.process_status[f'{broilerID}'] = {'status': 'active', 'message': f'There is an active cycle',
                                                       'details': self.process_data[f'{broilerID}']}
            else:
                # if len(messages) == 0:
                self.process_status[f'{broilerID}'] = {'status': 'Inactive',
                                                       'message': f'There is no active cycle for broiler {broilerID}'}
                continue
        if len(self.process_data) == 0:
            return

        self.archive_data()

    @staticmethod
    def save(db_name, status):
        if len(status) > 0:
            couch = connect_db(credentials['custom_url'])
            if db_name in couch:
                archive_db = couch[db_name]
                archive_db.save(status)
                if '_design/archive' not in archive_db:
                    archive_db['_design/archive'] = {  # create a view
                        "views": {
                            "by-date": {
                                "map": "function (doc) {\n  if(('broiler_info' in doc) && ('broiler_ID' in doc.broiler_info) && ('cycle_id' in doc.broiler_info) && doc.timestamp_name){\n    emit([doc.broiler_info.broiler_ID, doc.broiler_info.cycle_id, doc.timestamp_name], doc);\n  }\n}"
                            }
                        },
                        "language": "javascript"
                    }
            else:
                archive_db = couch.create(db_name)
                archive_db.save(status)
                archive_db['_design/archive'] = {  # create a view
                    "views": {
                        "by-date": {
                            "map": "function (doc) {\n  if(('broiler_info' in doc) && ('broiler_ID' in doc.broiler_info) && ('cycle_id' in doc.broiler_info) && doc.timestamp_name){\n    emit([doc.broiler_info.broiler_ID, doc.broiler_info.cycle_id, doc.timestamp_name], doc);\n  }\n}"
                        }
                    },
                    "language": "javascript"
                }

    def archive_data(self):
        if self.option == 1:  # for 10 minutes interval i.e. health and environmental deviations

            for i in self.process_data:
                data = self.process_data[i]

                # Environmental deviations
                practice = Practice()
                try:
                    status = practice.get_deviations_env(data)
                except Exception as e:
                    status = {"status": 500, "message": str(e)}
                    status.update(practice.broiler_info())

                db_name = f'archive_env_deviation_{i}_{data["cycle_id"]}'
                self.save(db_name=db_name, status=status)

                # Health
                h = HealthScore(data=data)
                try:
                    status = h.health()
                except Exception as e:
                    status = {"status": 500, "message": str(e)}
                    status.update(h.broiler_info())

                db_name = f'archive_health_{i}_{data["cycle_id"]}'
                self.save(db_name=db_name, status=status)

        elif self.option == 2:  # for daily interval
            for i in self.process_data:
                data = self.process_data[i]
                broilerID = data.get('broilerID')
                start_date = data.get('start_date')
                end_date = data.get('end_date')
                current_date = data.get('current_date')
                cycle_id = data.get('cycle_id')
                timezone = data.get('timezone', 2)
                noChickens = int(data.get('noChickens'))
                n_days = int(data.get('n_days'))

                # KPI deviations
                h = HealthScore(data=data)
                try:
                    weight = h.kpi_weight()
                    mortality = h.kpi_mortality()
                    fcr = h.kpi_fcr()

                    status = h.broiler_info()
                    status.update(weight)
                    status.update(mortality)
                    status.update(fcr)

                except Exception as e:
                    status = {"status": 500, "message": str(e)}
                    status.update(h.broiler_info())
                db_name = 'archive_kpi_deviations'
                self.save(db_name=db_name, status=status)

                # Uniformity
                h = HealthScore(data=data)
                try:
                    weight = h.weight_uniformity()
                    sensor = h.sensor_uniformity()

                    status = h.broiler_info()
                    status.update(weight)
                    status.update(sensor)
                except Exception as e:
                    status = {"status": 500, "message": str(e)}
                    status.update(h.broiler_info())
                db_name = 'archive_uniformity'
                self.save(db_name=db_name, status=status)

                # Activities
                h = HealthScore(data=data)
                try:
                    h.environmental_conditions()
                    vaccination = h.act_vaccination_record()
                    sanitation = h.act_sanitation_record()
                    mortality = h.act_mortality_record()
                    feed = h.act_feed_record()
                    weight = h.act_weight_record()

                    status = h.broiler_info()
                    status.update(vaccination)
                    status.update(sanitation)
                    status.update(mortality)
                    status.update(feed)
                    status.update(weight)
                except Exception as e:
                    status = {"status": 500, "message": str(e)}
                    status.update(h.broiler_info())
                db_name = 'archive_activities'
                self.save(db_name=db_name, status=status)

                # Devices
                h = HealthScore(data=data)
                try:
                    sensor = h.sensor_devices()
                    status = h.broiler_info()
                    status.update(sensor)
                except Exception as e:
                    status = {"status": 500, "message": str(e)}
                    status.update(h.broiler_info())
                db_name = 'archive_devices'
                self.save(db_name=db_name, status=status)

                # FCR
                FCR_model = FCR(broilerID=broilerID, noChickens=noChickens, start_date=start_date,
                                end_date=end_date,
                                current_date=current_date, cycle_id=cycle_id)
                try:
                    status = FCR_model.compute_FCRs()
                    status.update(FCR_model.broiler_info())
                    status.update(timezone_info(timezone=timezone))
                except Exception as e:
                    status = {'status': 400, 'message': "FCR error: " + str(e)}
                    status.update(FCR_model.broiler_info())
                    status.update(timezone_info(timezone=timezone))
                db_name = 'archive_fcr'
                self.save(db_name=db_name, status=status)

                # Mortality
                mort_model = MortalityPredictions(broilerID=int(broilerID), noChickens=noChickens,
                                                  start_date=start_date,
                                                  end_date=end_date, current_date=current_date, n_days=n_days,
                                                  cycle_id=cycle_id)
                try:
                    status = mort_model.predict()
                    status.update(mort_model.broiler_info())
                    status.update(timezone_info(timezone=timezone))
                except Exception as e:
                    status = {"broiler_info": {},
                         'status': 400, 'message': "Error: " + str(e)}
                    status.update(mort_model.broiler_info())
                    status.update(timezone_info(timezone=timezone))
                db_name = 'archive_mortality'
                self.save(db_name=db_name, status=status)

                # Weight
                growth_model = GrowthPredictions(broilerID=broilerID, start_date=start_date, end_date=end_date,
                                                 current_date=current_date, cycle_id=cycle_id)
                try:
                    growth_model._load_historic_growth()
                    growth_model._get_current_growth_array()

                    status = growth_model.fit_model()
                    status.update(growth_model.broiler_info())
                    status.update(timezone_info(timezone=timezone))
                except Exception as e:
                    status = {"broiler_info": {},
                              'status': 400, 'message': "Error: " + str(e)}
                    status.update(growth_model.broiler_info())
                    status.update(timezone_info(timezone=timezone))
                db_name = 'archive_weight'
                self.save(db_name=db_name, status=status)


archive = Archive()

# Problem: IBM cloud import this code twice
# Solution: Create an empty text file for first import, then second import will skip the code if file exists
filename = f'{dirname(abspath(__file__))}/tempo.txt'

if not exists(filename):
    open(filename, 'w').close()
    # scheduler for 10 minutes capture
    if not scheduler.get_job('minutes'):
        scheduler.add_job(archive.runner, trigger='interval', max_instances=1, id='minutes', minutes=10, args=[1])
    # scheduler for daily capture
    if not scheduler.get_job('daily'):
        scheduler.add_job(archive.runner, trigger='cron', max_instances=1, id='daily', hour='21', minute=00, args=[2])
else:
    remove(filename)

@app.route('/v1/api/autoarchive/status', methods=['POST'])
def auto_archive():
    """API to view status of auto archive"""
    return jsonify(archive.process_status)
