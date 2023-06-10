import os
from datetime import datetime

from flask import Flask, abort, session, request, redirect
from flask.json import jsonify
from flask_cors import CORS
# from flasgger import Swagger

app = Flask(__name__, template_folder="../public", static_folder="../public", static_url_path='')

credentials = {
    'username': 'f0d0d29c-7c5c-4212-9e05-6aa455b05adc-bluemix',
    'password': """e608b4ab60210ce0f9cf016e6d91efa45d515f3a57a2c2086b670f4cbdf30171""",
    'custom_url': 'https://f0d0d29c-7c5c-4212-9e05-6aa455b05adc-bluemix:e608b4ab60210ce0f9cf016e6d91efa45d515f3a57a2c2086b670f4cbdf30171@f0d0d29c-7c5c-4212-9e05-6aa455b05adc-bluemix.cloudantnosqldb.appdomain.cloud',
    'strain-list': '/chicken-strain/_design/strain/_view/list',
    'hist-list': '/historical/_design/hist/_view/list',
    'port': '50000',
    'chicken-strain': 'chicken-strain',
    'mortality': 'mortality',
    'strain': 'chicken-strain'
}
webapp = "https://app.omniofarm.com"
proxy = {'http': None, 'https': None}
auth = None

# Enable * for CORS
CORS(app, resources={r"*": {"origins": "*"}})

from server.routes import *
from server.services import *
from server.routes.index import connect_db


def log_error(status, typ):
    """Log errors to cloudant DB

    Args:
        status: message
        typ: which model type

    Returns:
        None
    """
    server_ = connect_db(url=credentials['custom_url'])
    temp = {
        "affects": typ,
        "status": status,
        "timestamp_name": datetime.utcnow().isoformat(),
        "timestamp": int(datetime.utcnow().timestamp() * 1000)
    }
    # if typ == "best-practice":
    date = datetime.utcnow().isoformat()
    if "error-logs" not in list(server_):
        server_.create("error-logs")
    db = server_['error-logs']

    if date in db:
        doc = db[date]
        doc.update(temp)
    db[date] = temp


initServices(app)
# app.config['SWAGGER'] = {
#     'title': 'OmnioFarm Local Insights APIs',
#     'uiversion': 3
# }
# swagger = Swagger(app)

if 'FLASK_LIVE_RELOAD' in os.environ and os.environ['FLASK_LIVE_RELOAD'] == 'true':
    import livereload

    app.debug = True
    server = livereload.Server(app.wsgi_app)
    server.serve(port=os.environ['port'], host=os.environ['host'])
