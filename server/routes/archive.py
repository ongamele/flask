from flask import jsonify, request

from server import app
from server import credentials
from server.routes.index import connect_db

import pandas as pd
from datetime import timedelta


class Archive:
    def __init__(self, data):
        self.broilerID = data.get('broilerID')
        self.start_date = data.get('start_date')
        self.end_date = data.get('end_date')
        self.cycle_id = data.get('cycle_id')

    def main(self, db_name):
        couch = connect_db(credentials['custom_url'])
        if db_name not in couch:
            return []
        db = couch[db_name]
        docs = db.view("archive/by-date", include_docs=False)
        x = docs[[str(self.broilerID), str(self.cycle_id), pd.to_datetime(self.start_date).date().isoformat()]:
                 [str(self.broilerID), str(self.cycle_id), (pd.to_datetime(self.end_date) + timedelta(days=1)).date().isoformat()]]
        results = []
        for i in list(x):
            doc = i['value']
            for j in ['_id', '_rev', 'broiler_info']:
                if j in doc:
                    doc.pop(j)
            results.append(doc)
        return results


@app.route('/v1/api/archive/deviations/activities', methods=['POST'])
def archive_activities():
    """API to archived data for activities"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400
    try:
        a = Archive(data)
        results = a.main('archive_activities')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/deviations/devices', methods=['POST'])
def archive_devices():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400
    try:
        a = Archive(data)
        results = a.main('archive_devices')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/deviations/environmental', methods=['POST'])
def archive_env_deviations():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main(f'archive_env_deviation_{broilerID}_{data["cycle_id"]}')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/fcr', methods=['POST'])
def archive_fcr():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main('archive_fcr')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/health', methods=['POST'])
def archive_health():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main(f'archive_health_{broilerID}_{data["cycle_id"]}')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/deviations/kpi', methods=['POST'])
def archive_kpi_deviations():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main('archive_kpi_deviations')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/predictive/mortality', methods=['POST'])
def archive_mortality():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main('archive_mortality')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/deviations/uniformity', methods=['POST'])
def archive_uniformity():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main('archive_uniformity')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}


@app.route('/v1/api/archive/predictive/weight', methods=['POST'])
def archive_weight():
    """API to get archived data for devices"""
    data = request.form.to_dict()
    broilerID = data.get('broilerID')
    if not broilerID:
        status = {"status": 400, "message": "broilerID missing"}
        return jsonify(status), 400
    start_date = data.get('start_date')
    if not start_date:
        status = {"status": 400, "message": "start_date missing"}
        return jsonify(status), 400
    end_date = data.get('end_date')
    if not end_date:
        status = {"status": 400, "message": "end_date missing"}
        return jsonify(status), 400
    cycle_id = data.get('cycle_id')
    if not cycle_id:
        status = {"status": 400, "message": "cycle_id missing"}
        return jsonify(status), 400

    try:
        a = Archive(data)
        results = a.main('archive_weight')
        return jsonify(results)
    except Exception as e:
        return {'status': 400, 'message': 'An error has occurred. Error ' + str(e)}
