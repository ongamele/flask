import requests

from server import app, credentials
from flask import request
from flask import jsonify
from flask import Response
from datetime import datetime
import couchdb
import yaml
import binascii
import pytz


def connect_db(url):
    """This function establish database connection

    Args:
        url (str): the full url in this format https://username:password@host:port/

    Returns:
        object/dict: Couch instance if connection is valid else error message.
    """
    try:
        couch = couchdb.Server(url)  # establish DB connection
        x = couch.version()
        if isinstance(x, float):
            return couch
    except binascii.Error:
        raise Exception(str({"status": 401, "message": "Could not connect."}))
    return couch


def upload_strain(url, f, db_table, file_id):
    """This function upload new chicken strain

    Args:
        url (str): the full url in this format https://username:password@host:port/
        f (file): stream file
        db_table (str): table name
        file_id (str): Unique ID of the file

    Returns:
        dict: success message if upload passed else error message.
    """
    yml = yaml.safe_load(f)
    couch = connect_db(url)
    if isinstance(couch, dict):
        return jsonify(couch), 401

    if db_table in list(couch):
        db = couch[db_table]  # select database
    else:
        status = {"status": 400, "message": ' database to save file not found'}
        return jsonify(status), 400

    date = datetime.now(tz=pytz.utc)
    doc = {
        'timestamp': int(date.timestamp()*1000),
        'timestamp_name': date.isoformat(),
        'file': yml,
    }
    if file_id in list(db):
        status = {"status": 400, "message": file_id + " file id name exists"}
        return jsonify(status), 400
    else:
        db[file_id] = doc
        status = {"status": 201, "message": "uploaded"}
        return jsonify(status), 201


@app.route('/')
def hello():
    """Test the server if is up

    tags:
      - API
    responses:
      200:
        description: Status
        schema:
          id: Status
          properties:
            status:
              type: string
              default: UP
    """
    status = {"status": "UP"}
    return jsonify(status), 200  # render_template('index.html')


@app.route('/v1/api/practice/create', methods=['POST'])
def api_practice_create():
    """Upload best practice file (bird strain)

    tags:
      - API
    parameters:
        - name: file
          in: query
          type: file
          required: true
          description: stream file
        - name: file_id
          in: query
          type: string
          required: true
          description: Unique ID of the file
    responses:
      201:
        description: Created
        schema:
          id: Created
          properties:
            status:
              type: integer
              default: 201
            message:
              type: string
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
      401:
        description: Unauthorized
        schema:
          id: Unauthorized
          properties:
            status:
              type: integer
              default: 401
            message:
              type: string
      415:
        description: Unsupported Media Type
        schema:
          id: Unsupported Media Type
          properties:
            status:
              type: integer
              default: 415
            message:
              type: string
      405:
        description: Method Not Allowed
        schema:
          id: Method Not Allowed
          properties:
            status:
              type: integer
              default: 405
            message:
              type: string
    """

    if request.method == 'POST':
        db_url = credentials['custom_url']
        db_table = credentials['chicken-strain']
        file_id = request.form.get('file_id')

        if not file_id:
            status = {"status": 412, "message": "file_id not valid."}
            return jsonify(status), 412

        if len(request.files) > 0:
            f = request.files.get('file')
            if f:
                if f.mimetype in ['text/plain', 'text/yaml']:
                    status = upload_strain(db_url, f, db_table, file_id)
                    return status
                else:
                    status = {"status": 415,
                              "message": str(f.mimetype) + ": not supported file type. Make sure to use "
                                                           "'text/plain' or 'text/yaml'"}
                    return jsonify(status), 415
            else:
                status = {"status": 400, "message": "Could not find the uploaded file."}
                return jsonify(status), 400
        else:
            status = {"status": 400,  "message": "Could not find the uploaded file."}
            return jsonify(status), 400
    else:
        status = {"status": 405, "message": request.method + " method not allowed"}
        return jsonify(status), 405


@app.route('/v1/api/practice/list', methods=['POST'])
def api_practice_list():
    """This function lists bird strain files

    tags:
      - API
    parameters:
        - name: db_table
          in: query
          type: string
          required: true
          description: database table name
    responses:
      200:
        description: List of Strain Files
        schema:
          id: List of Strain Files
          properties:
            _id:
              type: string
            timestamp:
              type: integer
            timestamp_name:
              type: string
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
      401:
        description: Unauthorized
        schema:
          id: Unauthorized
          properties:
            status:
              type: integer
              default: 401
            message:
              type: string
      405:
        description: Method Not Allowed
        schema:
          id: Method Not Allowed
          properties:
            status:
              type: integer
              default: 405
            message:
              type: string
    """
    if request.method == 'POST':

        db_url = credentials['custom_url']
        db_list = credentials['strain-list']

        data = requests.get(db_url+db_list)
        if data.status_code != 200:
            return jsonify({'status': 400, 'message': data.content}), 400
        try:
            data = data.json()
        except Exception as e:
            status = {'status': 400, 'message': 'Could not get list of strain files. Error' + str(e)}
            return jsonify(status), 400
        if 'rows' in data:
            row = data['rows']
            status = []
            for i in row:
                i.pop('id')
                i['stain_id'] = i.pop('key')
                status.append(i)
            return jsonify(status), 200
        else:
            status = {"status": 400, "message": 'No data found'}
            return jsonify(status), 400

    else:
        status = {"status": 405, "message": request.method + " method not allowed"}
        return jsonify(status), 405


@app.route('/v1/api/practice/delete', methods=['POST'])
def api_practice_delete():
    """Delete best practice file (bird strain)

    tags:
      - API
    parameters:
        - name: file_id
          in: query
          type: string
          required: true
          description: Unique ID of the file
    responses:
      201:
        description: Created
        schema:
          id: Created
          properties:
            status:
              type: integer
              default: 201
            message:
              type: string
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
      401:
        description: Unauthorized
        schema:
          id: Unauthorized
          properties:
            status:
              type: integer
              default: 401
            message:
              type: string
      405:
        description: Method Not Allowed
        schema:
          id: Method Not Allowed
          properties:
            status:
              type: integer
              default: 405
            message:
              type: string
    """
    if request.method == 'POST':
        db_url = credentials['custom_url']
        db_table = credentials['chicken-strain']

        file_id = request.form.get('file_id')
        if not file_id:
            status = {"status": 412, "message": "file_id not valid."}
            return jsonify(status), 412

        couch = connect_db(db_url)
        if isinstance(couch, dict):
            return jsonify(couch), 401
        if db_table in list(couch):
            db = couch[db_table]
            if file_id in list(db):
                del db[file_id]
                status = {"status": 201, "message": file_id + " deleted"}
                return jsonify(status), 201
            else:
                status = {"status": 400, "message": file_id + " file name not found"}
                return jsonify(status), 400
        else:
            status = {"status": 400, "message": db_table + " table name not found"}
            return jsonify(status), 400
    else:
        status = {"status": 405, "message": request.method + " method not allowed"}
        return jsonify(status), 405


@app.route('/v1/api/practice/get', methods=['POST'])
def api_practice_view():
    """Download best practice file (bird strain)

    tags:
      - API
    parameters:
        - name: file_id
          in: query
          type: string
          required: true
          description: Unique ID of the file
    responses:
      200:
        description: Ok
        schema:
          file: file
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
      401:
        description: Unauthorized
        schema:
          id: Unauthorized
          properties:
            status:
              type: integer
              default: 401
            message:
              type: string
      405:
        description: Method Not Allowed
        schema:
          id: Method Not Allowed
          properties:
            status:
              type: integer
              default: 405
            message:
              type: string
    """
    if request.method == 'POST':
        db_url = credentials['custom_url']
        db_table = credentials['chicken-strain']

        file_id = request.form.get('file_id')

        if not file_id:
            status = {"status": 412, "message": "file_id not valid."}
            return jsonify(status), 412

        couch = connect_db(db_url)
        if isinstance(couch, dict):
            return jsonify(couch), 401
        if db_table in couch:
            db = couch[db_table]
            if file_id in list(db):
                doc = db[file_id]
                doc = yaml.safe_dump(doc['file'])
                return Response(doc, mimetype='text/yaml')
            else:
                status = {"status": 400, "message": file_id + ' file name not found'}
                return jsonify(status), 400
        else:
            status = {"status": 400, "message": db_table + ' table name not found'}
            return jsonify(status), 400
    else:
        status = {"status": 405, "message": request.method + " method not allowed"}
        return jsonify(status), 405

###########
# def practice(request_):
#     """Build the request into a dictionary for best practice parameters
#     """
#     if request_.method == 'POST':
#         db_url = request_.form.get('db_url')
#         if not db_url:
#             return {"status": 412, "message": "db_url not valid."}
#
#         try:
#             db_url = base64.b64decode(db_url).decode()  # decode url to string from base64
#         except binascii.Error:
#             status = {"status": 401, "message": "Could not connect."}
#             return status
#
#         db_table = request_.form.get('db_table')
#         if not db_table:
#             return {"status": 412, "message": "db_table not valid."}
#
#         file_id = request_.form.get('file_id')
#         if not file_id:
#             return {"status": 412, "message": "file_id not valid."}
#
#         status = request_.form.get('status')
#         if not status:
#             return {"status": 412, "message": "status not valid."}
#
#         seconds = request_.form.get('seconds')
#         if not seconds:
#             return {"status": 412, "message": "seconds not valid."}
#
#         sourceDB = request_.form.get('sourceDB')
#         if not sourceDB:
#             return {"status": 412, "message": "sourceDB not valid."}
#
#         try:
#             sourceDB = base64.b64decode(sourceDB).decode()
#         except binascii.Error:
#             status = {"status": 412, "message": "sourceDB not valid."}
#             return status
#
#         sourceTable = request_.form.get('sourceTable')
#         if not sourceTable:
#             return {"status": 412, "message": "sourceTable not valid."}
#
#         strainDB = request_.form.get('strainDB')
#         if not strainDB:
#             return {"status": 412, "message": "strainDB not valid."}
#
#         try:
#             strainDB = base64.b64decode(strainDB).decode()
#         except binascii.Error:
#             status = {"status": 412, "message": "strainDB not valid."}
#             return status
#
#         strainTable = request_.form.get('strainTable')
#         if not strainTable:
#             return {"status": 412, "message": "strainTable not valid."}
#
#         strainID = request_.form.get('strainID')
#         if not strainID:
#             return {"status": 412, "message": "strainID not valid."}
#
#         cycleStart = request_.form.get('cycleStart')
#         if not cycleStart:
#             return {"status": 412, "message": "cycleStart not valid."}
#
#         cycleEnd = request_.form.get('cycleEnd')
#         if not cycleEnd:
#             return {"status": 412, "message": "cycleEnd not valid."}
#
#         broilerId = request_.form.get('broilerId')
#         if not broilerId:
#             return {"status": 412, "message": "broilerId not valid."}
#
#         doc = dict(
#             status=status,
#             seconds=seconds,
#             sourceDB=sourceDB,
#             sourceTable=sourceTable,
#             strainDB=strainDB,
#             strainTable=strainTable,
#             strainID=strainID,
#             cycleStart=cycleStart,
#             cycleEnd=cycleEnd,
#             broilerId=broilerId
#         )
#
#         return db_url, db_table, file_id, doc
#     else:
#         status = {"status": 405, "message": request_.method + " method not allowed"}
#         return status
#
#
# @app.route('/v1/api/practice/add', methods=['GET', 'POST'])
# def api_practice_add():
#     """Add best practice parameters
#     ---
#     tags:
#       - API
#     parameters:
#         - name: db_url
#           in: query
#           type: string
#           required: true
#           description: the full url in this format https://username:password@host:port/
#         - name: db_table
#           in: query
#           type: string
#           required: true
#           description: database table name
#         - name: file_id
#           in: query
#           type: string
#           required: true
#           description: Unique ID of the file
#         - name: save
#           in: query
#           type: string
#           required: true
#           description: yes or no
#         - name: seconds
#           in: query
#           type: integer
#           required: true
#           description: seconds - for update interval seconds
#         - name: sourceDB
#           in: query
#           type: string
#           required: true
#           description: the url of source database to read sensor data
#         - name: sourceTable
#           in: query
#           type: string
#           required: true
#           description: the database table name
#         - name: strainDB
#           in: query
#           type: string
#           required: true
#           description: the url of the bird strain database to use
#         - name: strainTable
#           in: query
#           type: string
#           required: true
#           description: the database table name
#         - name: strainID
#           in: query
#           type: string
#           required: true
#           description: bird strain file name
#         - name: cycleStart
#           in: query
#           type: string
#           required: true
#           description: the start of the cycle
#         - name: cycleEnd
#           in: query
#           type: string
#           required: true
#           description: the start of the cycle
#         - name: broilerId
#           in: query
#           type: string
#           required: true
#           description: the name of the cycle
#         - name: settings
#           in: query
#           type: dict
#           required: true
#           description: deviceTypes of the sensors mapped to the naming in strain files
#         - name: targetTable
#           in: query
#           type: string
#           required: true
#           description: database to save results of best practice
#     responses:
#       201:
#         description: Created
#         schema:
#           id: Created
#           properties:
#             status:
#               type: integer
#               default: 201
#             message:
#               type: string
#       400:
#         description: Bad Request
#         schema:
#           id: Bad Request
#           properties:
#             status:
#               type: integer
#               default: 400
#             message:
#               type: string
#       401:
#         description: Unauthorized
#         schema:
#           id: Unauthorized
#           properties:
#             status:
#               type: integer
#               default: 401
#             message:
#               type: string
#       405:
#         description: Method Not Allowed
#         schema:
#           id: Method Not Allowed
#           properties:
#             status:
#               type: integer
#               default: 405
#             message:
#               type: string
#       412:
#         description: Precondition Failed
#         schema:
#           id: Precondition Failed
#           properties:
#             status:
#               type: integer
#               default: 412
#             message:
#               type: string
#     """
#     results = practice(request)
#     if isinstance(results, dict):
#         if results['status'] == 405:
#             return jsonify(results), 405
#         elif results['status'] == 401:
#             return jsonify(results), 401
#         else:
#             return jsonify(results), 412
#     else:
#         db_url, db_table, file_id, doc = results
#     couch = connect_db(db_url)
#
#     if isinstance(couch, dict):
#         return jsonify(couch), 401
#     if db_table in list(couch):
#         db = couch[db_table]
#         if file_id not in list(db):
#             db[file_id] = doc
#             status = {"status": 201, "message": file_id + " added"}
#             return jsonify(status), 201
#         else:
#             status = {"status": 400, "message": file_id + " already exists"}
#             return jsonify(status), 400
#     else:
#         status = {"status": 400, "message": db_table + " table name not found"}
#         return jsonify(status), 400
#
#
# @app.route('/v1/api/practice/edit', methods=['GET', 'POST'])
# def api_practice_edit():
#     """Edit best practice parameters
#     ---
#     tags:
#       - API
#     parameters:
#         - name: db_url
#           in: query
#           type: string
#           required: true
#           description: the full url in this format https://username:password@host:port/ [base64encoded]
#         - name: db_table
#           in: query
#           type: string
#           required: true
#           description: database table name
#         - name: file_id
#           in: query
#           type: string
#           required: true
#           description: Unique ID of the file
#         - name: save
#           in: query
#           type: string
#           required: true
#           description: yes or no
#         - name: seconds
#           in: query
#           type: integer
#           required: true
#           description: seconds - for update interval seconds
#         - name: sourceDB
#           in: query
#           type: string
#           required: true
#           description: the url of source database to read sensor data
#         - name: sourceTable
#           in: query
#           type: string
#           required: true
#           description: the database table name
#         - name: strainDB
#           in: query
#           type: string
#           required: true
#           description: the url of the bird strain database to use
#         - name: strainTable
#           in: query
#           type: string
#           required: true
#           description: the database table name
#         - name: strainID
#           in: query
#           type: string
#           required: true
#           description: bird strain file name
#         - name: cycleStart
#           in: query
#           type: string
#           required: true
#           description: the start of the cycle
#         - name: cycleEnd
#           in: query
#           type: string
#           required: true
#           description: the start of the cycle
#         - name: broilerId
#           in: query
#           type: string
#           required: true
#           description: the name of the cycle
#         - name: settings
#           in: query
#           type: dict
#           required: true
#           description: deviceTypes of the sensors mapped to the naming in strain files
#         - name: targetTable
#           in: query
#           type: string
#           required: true
#           description: database to save results of best practice
#     responses:
#       201:
#         description: Created
#         schema:
#           id: Created
#           properties:
#             status:
#               type: integer
#               default: 201
#             message:
#               type: string
#       400:
#         description: Bad Request
#         schema:
#           id: Bad Request
#           properties:
#             status:
#               type: integer
#               default: 400
#             message:
#               type: string
#       401:
#         description: Unauthorized
#         schema:
#           id: Unauthorized
#           properties:
#             status:
#               type: integer
#               default: 401
#             message:
#               type: string
#       405:
#         description: Method Not Allowed
#         schema:
#           id: Method Not Allowed
#           properties:
#             status:
#               type: integer
#               default: 405
#             message:
#               type: string
#       412:
#         description: Precondition Failed
#         schema:
#           id: Precondition Failed
#           properties:
#             status:
#               type: integer
#               default: 412
#             message:
#               type: string
#     """
#     results = practice(request)
#     if isinstance(results, dict):
#         if results['status'] == 405:
#             return jsonify(results), 405
#         elif results['status'] == 401:
#             return jsonify(results), 401
#         else:
#             return jsonify(results), 412
#     else:
#         db_url, db_table, file_id, doc = results
#     couch = connect_db(db_url)
#
#     if isinstance(couch, dict):
#         return jsonify(couch), 401
#     if db_table in list(couch):
#         db = couch[db_table]
#         if file_id in list(db):
#             document = db[file_id]
#             document.update(doc)
#             db[file_id] = document
#             status = {"status": 201, "message": file_id + " updated"}
#         else:
#             status = {"status": 400, "message": file_id + ' does not exists'}
#             return jsonify(status), 400
#     else:
#         status = {"status": 400, "message": db_table + ' table name not found'}
#         return jsonify(status), 400
#
#     return jsonify(status), 201
#
#
# @app.route('/v1/api/practice/download', methods=['GET', 'POST'])
# def api_practice_download():
#     """Download best practice parameters
#     ---
#     tags:
#       - API
#     parameters:
#         - name: db_url
#           in: query
#           type: string
#           required: true
#           description: the full url in this format https://username:password@host:port/ [base64encoded]
#         - name: db_table
#           in: query
#           type: string
#           required: true
#           description: database table name
#         - name: file_id
#           in: query
#           type: string
#           required: true
#           description: Unique ID of the file
#     responses:
#       201:
#         description: Download Strain
#         schema:
#           id: Download Strain
#           properties:
#             db_url:
#               type: string
#             db_table:
#               type: string
#             file_id:
#               type: string
#             status:
#               type: string
#             seconds:
#               type: integer
#             sourceDB:
#               type: string
#             sourceTable:
#               type: string
#             strainDB:
#               type: string
#             strainTable:
#               type: string
#             strainID:
#               type: string
#             cycleStart:
#               type: string
#             cycleEnd:
#               type: string
#             broilerId:
#               type: string
#       400:
#         description: Bad Request
#         schema:
#           id: Bad Request
#           properties:
#             status:
#               type: integer
#               default: 400
#             message:
#               type: string
#       401:
#         description: Unauthorized
#         schema:
#           id: Unauthorized
#           properties:
#             status:
#               type: integer
#               default: 401
#             message:
#               type: string
#       405:
#         description: Method Not Allowed
#         schema:
#           id: Method Not Allowed
#           properties:
#             status:
#               type: integer
#               default: 405
#             message:
#               type: string
#       412:
#         description: Precondition Failed
#         schema:
#           id: Precondition Failed
#           properties:
#             status:
#               type: integer
#               default: 412
#             message:
#               type: string
#
#     """
#     if request.method == 'POST':
#         db_url = request.form.get('db_url')
#         if not db_url:
#             return {"status": 412, "message": "db_url not valid."}, 412
#
#         try:
#             db_url = base64.b64decode(db_url).decode()  # decode url to string from base64
#         except binascii.Error:
#             status = {"status": 401, "message": "Could not connect."}
#             return jsonify(status), 401
#
#         db_table = request.form.get('db_table')
#         if not db_table:
#             return {"status": 412, "message": "db_table not valid."}, 412
#
#         file_id = request.form.get('file_id')
#         if not file_id:
#             return {"status": 412, "message": "file_id not valid."}, 412
#
#         couch = connect_db(db_url)
#
#         if isinstance(couch, dict):
#             return jsonify(couch), 401
#         if db_table in list(couch):
#             db = couch[db_table]
#             if file_id in list(db):
#                 document = db[file_id]
#                 status = document
#             else:
#                 status = {"status": 400, "message": file_id + ' does not exists'}
#                 return jsonify(status), 400
#         else:
#             status = {"status": 400, "message": db_table + ' table name not found'}
#             return jsonify(status), 400
#     else:
#         status = {"status": 405, "message": request.method + " method not allowed"}
#         return jsonify(status), 405
#     return jsonify(status), 201
#
#
# @app.route('/v1/api/practice/delete', methods=['GET', 'POST'])
# def api_practice_delete():
#     """Delete best practice parameters
#
#     ---
#     tags:
#       - API
#     parameters:
#         - name: db_url
#           in: query
#           type: string
#           required: true
#           description: the full url in this format https://username:password@host:port/ [base64encoded]
#         - name: db_table
#           in: query
#           type: string
#           required: true
#           description: database table name
#         - name: file_id
#           in: query
#           type: string
#           required: true
#           description: Unique ID of the file
#     responses:
#       201:
#         description: Created
#         schema:
#           id: Created
#           properties:
#             status:
#               type: integer
#               default: 201
#             message:
#               type: string
#       400:
#         description: Bad Request
#         schema:
#           id: Bad Request
#           properties:
#             status:
#               type: integer
#               default: 400
#             message:
#               type: string
#       401:
#         description: Unauthorized
#         schema:
#           id: Unauthorized
#           properties:
#             status:
#               type: integer
#               default: 401
#             message:
#               type: string
#       405:
#         description: Method Not Allowed
#         schema:
#           id: Method Not Allowed
#           properties:
#             status:
#               type: integer
#               default: 405
#             message:
#               type: string
#       412:
#         description: Precondition Failed
#         schema:
#           id: Precondition Failed
#           properties:
#             status:
#               type: integer
#               default: 412
#             message:
#               type: string
#     """
#     if request.method == 'POST':
#         db_url = request.form.get('db_url')
#         if not db_url:
#             return {"status": 412, "message": "db_url not valid."}, 412
#
#         try:
#             db_url = base64.b64decode(db_url).decode()  # decode url to string from base64
#         except binascii.Error:
#             status = {"status": 401, "message": "Could not connect."}
#             return jsonify(status), 401
#
#         db_table = request.form.get('db_table')
#         if not db_table:
#             return {"status": 412, "message": "db_table not valid."}, 412
#
#         file_id = request.form.get('file_id')
#         if not file_id:
#             return {"status": 412, "message": "file_id not valid."}, 412
#
#         couch = connect_db(db_url)
#
#         if isinstance(couch, dict):
#             return jsonify(couch), 401
#         if db_table in list(couch):
#             db = couch[db_table]
#             if file_id in list(db):
#                 del db[file_id]
#                 status = {"status": 201, "message": file_id + " deleted"}
#             else:
#                 status = {"status": 400, "message": file_id + ' does not exists'}
#                 return jsonify(status), 400
#         else:
#             status = {"status": 400, "message": db_table + ' table name not found'}
#             return jsonify(status), 400
#     else:
#         status = {"status": 405, "message": request.method + " method not allowed"}
#         return jsonify(status), 405
#     return jsonify(status), 201


@app.errorhandler(404)
@app.route("/404")
def page_not_found():
    status = {"status": 404, "message": "request not found"}
    return jsonify(status), 404


@app.errorhandler(500)
@app.route("/500")
def requests_error():
    status = {"status": 500, "message": "internal server error"}
    return jsonify(status), 500  # app.send_static_file('500.html')
