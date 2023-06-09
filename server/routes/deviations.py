from flask import request, jsonify

from server import app
from server.routes.health_score import HealthScore
from server.routes.practice import Practice


@app.route('/v1/api/practice/deviations/activities', methods=['POST'])
def api_activities():
    """API to get deviations for broiler activities"""
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
    if not data.get('parent_flock_age'):
        status = {"status": 400, "message": "parent_flock_age missing"}
        return jsonify(status), 400

    try:
        h = HealthScore(data=data)
        d = h.environmental_conditions()
        if 'status' in d and d['status'] == 400:
            return jsonify(d)
        vaccination = h.act_vaccination_record()
        sanitation = h.act_sanitation_record()
        mortality = h.act_mortality_record()
        feed = h.act_feed_record()
        weight = h.act_weight_record()

        b = h.broiler_info()
        b.update(vaccination)
        b.update(sanitation)
        b.update(mortality)
        b.update(feed)
        b.update(weight)
        return jsonify(b)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/practice/deviations/uniformity', methods=['POST'])
def api_uniformity():
    """API to get weight and sensor uniformity deviations"""
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
    if not data.get('parent_flock_age'):
        status = {"status": 400, "message": "parent_flock_age missing"}
        return jsonify(status), 400

    try:
        h = HealthScore(data=data)
        weight = h.weight_uniformity()
        sensor = h.sensor_uniformity()
        if 'status' in sensor and sensor['status'] == 400:
            sensor = {
                'sensor_uniformity': sensor
            }

        b = h.broiler_info()
        b.update(weight)
        b.update(sensor)
        return jsonify(b)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        return jsonify(status)


@app.route('/v1/api/practice/deviations/kpi', methods=['POST'])
def api_kpi():
    """API to get KPI deviations"""
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

    h = HealthScore(data=data)
    try:
        weight = h.kpi_weight()
        mortality = h.kpi_mortality()
        fcr = h.kpi_fcr()

        b = h.broiler_info()
        b.update(weight)
        b.update(mortality)
        b.update(fcr)
        return jsonify(b)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        status.update(h.broiler_info())
    return jsonify(status)


@app.route('/v1/api/practice/deviations/env', methods=['POST'])
def api_env():
    """Get environmental deviations
    tags:
      - API
    responses:
    """
    practice = Practice()

    try:
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
        if not data.get('parent_flock_age'):
            status = {"status": 400, "message": "parent_flock_age missing"}
            return jsonify(status), 400
        if not data.get('timezone', 2):
            status = {"status": 400, "message": "timezone missing"}
            return jsonify(status), 400
        results = practice.get_deviations_env(data)
        return results
    except Exception as e:
        status = {"status": 500, "message": str(e)}
        # practice.log_error(status=status, typ="best-practice")
        return jsonify(status), 500


@app.route('/v1/api/practice/deviations/devices', methods=['POST'])
def api_devices():
    """API for devices deviations"""

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

    try:
        h = HealthScore(data=data)
        sensor = h.sensor_devices()

        b = h.broiler_info()
        b.update(sensor)

        return jsonify(b)
    except Exception as e:
        status = {"status": 500, "message": str(e)}
    return jsonify(status)
