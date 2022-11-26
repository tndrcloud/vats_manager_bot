import flask
import psycopg2
import os
import datetime
import schedule_rest
import json


TOKENS = False

app = flask.Flask(__name__)

def sql_request(sql, data_python=False):
    connect = psycopg2.connect(dbname=False, user=False, password=False, host=False, port=False)
    cursor = connect.cursor()

    # data_python должен быть в формате (obj,) или[(obj1, obj2), (obj3, obj4)]
    if data_python:
        cursor.execute(sql, data_python)
    else:
        cursor.execute(sql)
    if sql[:6] == 'SELECT':
        result = cursor.fetchall()
        connect.close()
        return result

    elif sql[:6] == 'UPDATE' or sql[:6] == 'DELETE' or sql[:6] == 'INSERT':
        connect.commit()
        connect.close()
        return True

@app.errorhandler(404)
def not_found(error):
    return {"error": "Not found"}, 404

@app.before_request
def limit_remote_addr():
    token = flask.request.headers.get("X-Client-ID")
    if token not in TOKENS:
        return '<b>Fuck the fuck out of here, motherfucker!</b>', 403  # Forbidden

@app.route('/ping', methods=['GET'])
def ping():
    return {"response":"Pong!"}, 200

@app.route('/schedule/holiday', methods=['GET'])
def get_schedule_holiday():
    result = schedule_rest.check_holiday(datetime.datetime.now(), get_users()['users'])
    return {'result': result}

@app.route('/get_users', methods=['GET'])
def get_users():
    sql = "SELECT * FROM users"
    result = sql_request(sql)
    return {'users': result}

@app.route('/analytics_nttm/tickets_vendor', methods=['POST'])
def set_analytics_nttm_tickets_vendor():
    data_json = flask.request.json
    sql = f"UPDATE analytics_nttm SET value = '{data_json}' WHERE type = 'tickets_vendor'"
    sql_request(sql)
    return {'result': 'database update'}

@app.route('/analytics_nttm/direction', methods=['POST'])
def set_analytics_nttm_direction():
    data_json = flask.request.json
    sql = f"UPDATE analytics_nttm SET value = '{data_json}' WHERE type = 'direction'"
    sql_request(sql)

    return {'result': 'database update'}

@app.route('/call_events/', methods=['POST'])
def call_events():
    event = flask.request.json
    if event:
        if event['type'] == 'outbound' or event['type'] == 'incoming':
            try:
                connect = psycopg2.connect(dbname=False, user=False, password=False, host=False, port=False)
                cursor = connect.cursor()

                if event['state'] == 'connected':
                    cursor.execute(f"""INSERT INTO calls
                                      VALUES ('{event['type']}', '{event['state']}', '{event['session_id']}', '{event['timestamp']}',
                                      'None', '{event['from_number']}', '{event['request_number']}')"""
                                   )
                    connect.commit()
                elif event['state'] == 'disconnected':
                    sql = f"""
                    UPDATE calls 
                    SET end_time = '{event['timestamp']}',
                    state = '{event['state']}'
                    WHERE session_id = '{event['session_id']}'
                    """
                    cursor.execute(sql)
                    connect.commit()
                else:
                    connect.close()
                    return 'wrong state call, correct state "connected" and "disconnected"', 201

                connect.close()
                return "data saved", 200

            except Exception as error:
                connect.close()
                print(error)
                return 'server error', 500

        return 'wrong type call', 201

    else:
        return 'data not found', 400
