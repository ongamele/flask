from os.path import dirname, abspath

import requests
from flask import jsonify, request, send_file
from pandas import ExcelWriter

from server import app, credentials, proxy
from server.routes.index import connect_db
import pandas as pd
import numpy as np

# from server import
from datetime import datetime


mimes = ['application/vnd.ms-excel',
         'application/msexcel',
         'application/x-msexcel',
         'application/x-ms-excel',
         'application/x-excel',
         'application/x-dos_ms_excel',
         'application/xls',
         'application/x-xls',
         'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']


@app.route('/v1/api/history/create', methods=['POST'])
def history_create():
    try:
        data = request.form.to_dict()
        broilerID = int(data.get('broilerID'))
        if not broilerID:
            status = {"status": 400, "message": "broilerID missing"}
            return jsonify(status), 400

        if len(request.files) > 0:
            f = request.files.get('file')
            if f and f.mimetype in mimes:
                y = pd.ExcelFile(f)

                data = {'timestamp': int(datetime.utcnow().timestamp()*1000),
                        'timestamp_name': datetime.utcnow().isoformat().split('.')[0]}
                temp = {}
                if len(y.sheet_names) == 0:
                    status = {"status": 400, "message": "The excel file does not have sheet names"}
                    return jsonify(status), 400
                for i in y.sheet_names:
                    header = []
                    for j in y.parse(i, nrows=0).columns:
                        if type(j) == str and 'Unnamed:' in j:
                            continue
                        if isinstance(j, datetime):
                            j = j.date().isoformat()
                        header.append(j)

                    df = y.parse(i, skiprows=1)

                    if "AGE" not in df.columns:
                        status = {"status": 400,
                                  "message": f"The excel file does not have AGE in sheet name {i}. "
                                             f"Add AGE and try again"}
                        return status
                    if "%CUMM" not in df.columns:
                        status = {"status": 400,
                                  "message": f"The excel file does not have %CUMM in sheet name {i}. "
                                             f"Add %CUMM and try again"}
                        return status
                    if 'WEIGHT' not in df.columns:
                        status = {"status": 400,
                                  "message": f"The excel file does not have WEIGHT in sheet name {i}. "
                                             f"Add WEIGHT and try again"}
                        return status
                    df['DATE'] = df['DATE'].apply(lambda x: pd.to_datetime(x).date().isoformat())
                    df = df.replace({np.nan: None})
                    temp[i] = {
                        'header': header,
                        'doc': [j[1].to_dict() for j in df.iterrows()]
                    }
                data['file'] = temp
                server = connect_db(url=credentials['custom_url'])

                if "historical" in list(server):
                    db = server['historical']
                    if str(broilerID) in db:
                        doc = db[str(broilerID)]
                        doc.update(data)
                        return {"status": 200, "message": f"Updated historical data from broiler ID: {broilerID}"}
                    else:
                        db[str(broilerID)] = data
                        return {"status": 200, "message": f"Created historical data from broiler ID: {broilerID}"}
                else:
                    server.create("historical")
                    db = server['historical']
                    db[str(broilerID)] = data
                return {"status": 200, "message": "Success"}
            else:
                status = {'status': 400, 'message': f'{f.mimetype} file type not supported'}
                return jsonify(status), 400
        else:
            status = {'status': 400, 'message': 'File not found'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": "Create historical. Error: " + str(e)}
        return jsonify(status), 500


@app.route('/v1/api/history/list', methods=['POST'])
def history_list():
    try:
        db_url = credentials['custom_url']
        db_list = credentials['hist-list']
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
                i['broilerID'] = i.pop('key')
                status.append(i)
            return jsonify(status), 200
        else:
            status = {"status": 400, "message": 'No data found'}
            return jsonify(status), 400
    except Exception as e:
        return jsonify({'status': 400, 'message': 'Error: ' + str(e)})


@app.route('/v1/api/history/delete', methods=['POST'])
def history_delete():
    try:
        data = request.form.to_dict()
        broilerID = int(data.get('broilerID'))
        if not broilerID:
            status = {"status": 400, "message": "broilerID missing"}
            return jsonify(status), 400
        server = connect_db(url=credentials['custom_url'])
        if "historical" not in list(server):
            status = {'status': 400, 'message': 'No historical data exists'}
            return jsonify(status), 400
        db = server['historical']
        if str(broilerID) in db:
            del db[str(broilerID)]
            return {'status': 200, 'message': 'Success'}
        else:
            status = {'status': 400, 'message': f'Historical data for broiler {broilerID} not found'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": "Delete historical. Error: " + str(e)}
        return jsonify(status), 500


@app.route('/v1/api/history/get', methods=['POST'])
def history_get():
    try:
        data = request.form.to_dict()
        broilerID = int(data.get('broilerID'))
        if not broilerID:
            status = {"status": 400, "message": "broilerID missing"}
            return jsonify(status), 400
        server = connect_db(url=credentials['custom_url'])
        if "historical" not in list(server):
            status = {'status': 400, 'message': 'No historical data exists'}
            return jsonify(status), 400
        db = server['historical']
        if str(broilerID) in db:
            with ExcelWriter(f'{dirname(abspath(__file__))}/Production_Records_broiler-{broilerID}.xlsx') as writer:
                doc = db[str(broilerID)]['file']
                for i in doc:
                    pd.DataFrame(doc[i]['header']).T.to_excel(writer, sheet_name=i, index=None, header=None)
                    pd.DataFrame(doc[i]['doc']).to_excel(writer, sheet_name=i,  index=False, startrow=1)
            return send_file(f'{dirname(abspath(__file__))}/Production_Records_broiler-{broilerID}.xlsx')
        else:
            status = {'status': 400, 'message': f'Historical data for broiler {broilerID} not found'}
            return jsonify(status), 400
    except Exception as e:
        status = {"status": 500, "message": "Get historical. Error: " + str(e)}
        return jsonify(status), 500
