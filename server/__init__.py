import os
from datetime import datetime

from flask import Flask, abort, session, request, redirect
from flask.json import jsonify
from flask_cors import CORS
# from flasgger import Swagger

app = Flask(__name__, template_folder="../public", static_folder="../public", static_url_path='')

credentials = {
    'username': 'apikey-v2-1w08ohl9i74lknm6th5iynb41xv32qwtcfepx8s45gis',
    'password': '521a26d377fb7a249542ec9977d245fc',
    'custom_url': 'https://apikey-v2-1w08ohl9i74lknm6th5iynb41xv32qwtcfepx8s45gis:521a26d377fb7a249542ec9977d245fc@f3fd4444-63fe-4117-8dad-6dfe6220c3e1-bluemix.cloudantnosqldb.appdomain.cloud',
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
