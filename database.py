import psycopg2
import os
import json
import telegram
import time
import threading
import datetime
from telegram.ext import *


def sql_request(sql, data_python=()):
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

    elif sql[:6] in ('UPDATE', 'DELETE', 'INSERT', 'CREATE'):
        connect.commit()
        connect.close()
        return True


def get_users_access(access='user'):
    if access == 'root':
        sql = "SELECT username_tg FROM users WHERE access = 'root'"
    elif access == 'admin':
        sql = "SELECT username_tg FROM users WHERE access = 'admin' OR access = 'root'"
    elif access == 'user':
        sql = "SELECT username_tg FROM users"
    elif access == 'access_4':
        sql = "SELECT username_tg FROM users"
        users = [user[0] for user in sql_request(sql)]
        return [*users, *Settings.get_access_4()]
    elif access == 'access_5':
        sql = "SELECT username_tg FROM users"
        users = [user[0] for user in sql_request(sql)]
        return [*users, *Settings.get_access_4(), *Settings.get_access_5()]

    return [user[0] for user in sql_request(sql)]


def auth_user(access):
    """
    root - разработчики
    admin - гсп
    user - инженеры
    access_4 - ДС ОТТ
    access_5 - ОЭП
    """

    def decorator_auth(func_to_decorate):
        def new_func(*original_args, **original_kwargs):
            for arg in original_args:
                if type(arg) == telegram.update.Update:
                    if arg.effective_user.username in get_users_access(access):
                        return func_to_decorate(*original_args, **original_kwargs)
                    else:
                        return send_unauthorized_message(*original_args, **original_kwargs)

        return new_func

    return decorator_auth


def checking_privileges(user, access):
    if user in get_users_access(access):
        return True
    else:
        return False


def send_unauthorized_message(*original_args, msg_id=None):
    for arg in original_args:
        if isinstance(arg, telegram.update.Update):
            update = arg
            break
    else:
        raise AttributeError("Не найден update")

    for arg in original_args:
        if isinstance(arg, CallbackContext):
            context = arg
            break
    else:
        raise AttributeError("Не найден context")

    if not msg_id:
        if update.callback_query:
            context.bot.answer_callback_query(update.callback_query.id, text="Отказано в доступе!")
        else:
            answerMes = context.bot.send_message(chat_id=update.effective_chat.id, text="Отказано в доступе!")
            threading.Thread(target=send_unauthorized_message, args=(update, context),
                             kwargs={'msg_id': answerMes['message_id']}, daemon=True).start()
    else:
        time.sleep(5)
        context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_id)


class Settings:
    # test - тестовая среда (без отправки уведомлений и т.д.)
    # prod - продуктивная среда
    _working_mode = None
    sql_request("CREATE TABLE if not exists settings (type text, value text, name text)")

    @classmethod
    def get_working_mode(cls):
        if cls._working_mode:
            return cls._working_mode
        else:
            sql = "SELECT value FROM settings WHERE type = 'working_mode'"
            cls._working_mode = sql_request(sql)[0][0]
            return cls._working_mode

    @classmethod
    def get_token(cls):
        if cls._working_mode == 'prod':
            sql = "SELECT value FROM settings WHERE type = 'token'"
        else:
            sql = "SELECT value FROM settings WHERE type = 'token_test'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_chat_id_events():
        sql = "SELECT value FROM settings WHERE type = 'chat_id_bug'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_chat_id_analyst():
        sql = f"SELECT value FROM settings WHERE type = 'chat_id_analystics'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_alarm_wait_inc():
        sql = "SELECT value FROM settings WHERE type = 'alarm_wait_inc'"
        return int(sql_request(sql)[0][0])

    @staticmethod
    def get_chat_id_svetofor():
        sql = "SELECT value FROM settings WHERE type = 'chat_id_svetofor'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_chat_id_duty():
        sql = "SELECT value FROM settings WHERE type = 'chat_id_duty'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_nttm_filter_direction():
        sql = "SELECT value FROM settings WHERE type = 'nttm_filter_direction'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_nttm_filter_vendor():
        sql = "SELECT value FROM settings WHERE type = 'nttm_filter_vendor'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_nttm_filter_queue():
        sql = "SELECT value FROM settings WHERE type = 'nttm_filter_queue'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_access_4():
        sql = "SELECT value FROM settings WHERE type = 'access_4'"
        return sql_request(sql)[0][0].split(';')

    @staticmethod
    def get_access_5():
        sql = "SELECT value FROM settings WHERE type = 'access_5'"
        return sql_request(sql)[0][0].split(';')

    @staticmethod
    def get_types_and_names():
        sql = "SELECT type, name FROM settings"
        return sql_request(sql)

    @staticmethod
    def get_setting(setting):
        sql = f"SELECT value FROM settings WHERE type = '{setting}'"
        return sql_request(sql)[0][0]

    @staticmethod
    def set_setting(setting, value):
        sql = f"UPDATE settings SET value = '{value}' WHERE type = '{setting}'"
        sql_request(sql)

    @staticmethod
    def get_absence_minutes():
        sql = f"SELECT value FROM settings WHERE type = 'absence_minutes';"
        return int(sql_request(sql)[0][0])


class Data:
    sql_request("CREATE TABLE if not exists data (type text, value text)")

    @staticmethod
    def get_tickets_vendor(codec):
        sql = f"SELECT value FROM data WHERE type = 'tickets_vendor'"
        response = sql_request(sql)
        if len(response) > 0:
            data = json.loads(response[0][0])
            if len(data['tickets']) > 0:
                file_csv = f'Последнее обновление;{data["update_time"]}\r\n' \
                           f'ticket;incidents;description;description2\r\n'.encode(codec, errors='ignore')
                for ticket in data['tickets']:
                    file_csv += f"{ticket};{', '.join(data['tickets'][ticket]['inc'])};" \
                                f"{';'.join(data['tickets'][ticket]['description'])}\r\n".encode(codec, errors='ignore')
                return data["update_time"], file_csv
        return None, None

    @staticmethod
    def set_tickets_vendor(tickets_vendor):
        tickets_json = json.dumps(tickets_vendor, ensure_ascii=False)
        sql = f"UPDATE data SET value = '{tickets_json}' WHERE type = 'tickets_vendor'"
        sql_request(sql)

    @staticmethod
    def get_si_inc():
        sql = f"SELECT value FROM data WHERE type = 'si_tickets'"
        response = sql_request(sql)
        if response:
            return json.loads(response[0][0])
        else:
            return response

    @classmethod
    def add_si_inc(cls, inc_si: str):
        curr = cls.get_si_inc()
        new = [inc_si]
        if curr:
            new.extend(curr)
        print(json.dumps(new))
        sql = f"UPDATE data SET value = '{json.dumps(new)}' WHERE type = 'si_tickets'"
        sql_request(sql)

    @classmethod
    def del_si_inc(cls, inc_si: str):
        curr = cls.get_si_inc()
        if curr:
            curr.remove(int(inc_si))
        sql = f"UPDATE data SET value = '{json.dumps(curr)}' WHERE type = 'si_tickets'"
        sql_request(sql)


class Users:
    sql_request("""CREATE TABLE if not exists users (username_tg text, access text, username_vats text, 
                    full_name text, username_skuf text, autofaq_id text, chat_id text, analytics BOOLEAN)""")

    @staticmethod
    def create_user_tg(username_tg):
        sql = f"INSERT INTO users(username_tg) VALUES ('{username_tg}')"
        sql_request(sql)

    @staticmethod
    def get_user_chat_id(username_tg):
        sql = f"SELECT chat_id FROM users WHERE username_tg = '{username_tg}'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_custom_params(*args):
        params = ', '.join(args)
        sql = f"SELECT {params} FROM users;"
        return sql_request(sql)

    @staticmethod
    def get_users_info():
        sql = "SELECT * FROM users"
        return sql_request(sql)

    @staticmethod
    def get_fullname_by_username_tg(username_tg):
        sql = f"SELECT full_name FROM users WHERE username_tg = '{username_tg}'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_username_skuf(username_tg):
        sql = f"SELECT username_skuf FROM users WHERE username_tg = '{username_tg}'"
        return sql_request(sql)[0][0]

    @staticmethod
    def get_users_column_name():
        sql = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'users';"
        result = []
        for column in sql_request(sql):
            result.append(column[0])
        return result

    @staticmethod
    def get_user_username_vats(username_tg):
        sql = f"SELECT username_vats FROM users WHERE username_tg = '{username_tg}'"
        return sql_request(sql)[0]

    @staticmethod
    def get_user_autofaq_id(username_tg):
        sql = f"SELECT autofaq_id FROM users WHERE username_tg = '{username_tg}'"
        return sql_request(sql)[0][0]

    @staticmethod
    def change_analytics(username_tg):
        sql = f"SELECT analytics FROM users WHERE username_tg = '{username_tg}'"
        curr_value = sql_request(sql)[0][0]
        if curr_value:
            new_value = 0
        else:
            new_value = 1
        sql = f"UPDATE users SET analytics = '{new_value}' WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def set_user_username_tg(old_username_tg, new_username_tg):
        sql = f"UPDATE users SET username_tg = '{new_username_tg}' WHERE username_tg = '{old_username_tg}'"
        sql_request(sql)

    @staticmethod
    def set_user_access(username_tg, access):
        sql = f"UPDATE users SET access = '{access}' WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def set_user_username_vats(username_tg, username_vats):
        if username_vats == 'None':
            sql = f"UPDATE users SET username_vats = 'None' WHERE username_tg = '{username_tg}'"
            sql_request(sql)
        else:
            sql = f"UPDATE users SET username_vats = '{username_vats}' WHERE username_tg = '{username_tg}'"
            sql_request(sql)

    @staticmethod
    def set_user_full_name(username_tg, full_name):
        sql = f"UPDATE users set full_name = '{full_name}' WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def set_user_username_skuf(username_tg, username_skuf):
        sql = f"UPDATE users set username_skuf = '{username_skuf}' WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def set_user_autofaq_id(username_tg, autofaq_id):
        sql = f"UPDATE users SET autofaq_id = '{autofaq_id}' WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def set_user_chat_id(username_tg, chat_id):
        sql = f"UPDATE users SET chat_id = '{chat_id}' WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def del_user(username_tg):
        sql = f"DELETE FROM users WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def del_user_chat_id(username_tg):
        sql = f"UPDATE users SET chat_id = null WHERE username_tg = '{username_tg}'"
        sql_request(sql)

    @staticmethod
    def check_chat_id(username_tg):
        sql = f"SELECT chat_id FROM users WHERE username_tg = '{username_tg}'"
        result = sql_request(sql)
        if result[0][0] == None or result[0][0] == 'None':
            return False
        else:
            return True


class Calls:
    sql_request("""CREATE TABLE if not exists calls (type text, state text, session_id text, start_time text, 
    end_time text, from_number text, request_number text)""")

    @staticmethod
    def get_calls_bd():
        connect = psycopg2.connect(dbname=False, user=False, password=False, host=False, port=False)
        cursor = connect.cursor()

        cursor.execute("SELECT * FROM calls ORDER BY start_time DESC")
        while True:
            call = cursor.fetchone()
            if call:
                yield call
            else:
                break
        connect.close()

    @staticmethod
    def fix_calls():
        # Обновление вызовов принудительно
        sql = f"""UPDATE calls SET end_time = '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}', 
                        state = 'disconnected' WHERE state = 'connected';"""
        sql_request(sql)


class Lunches:
    sql_request("""CREATE TABLE if not exists lunches (full_name text, start_time text, 
                    end_time text, duration text, state text)""")

    @staticmethod
    def start_lunch(full_name):
        sql = f"""INSERT INTO lunches VALUES ('{full_name}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 
                        'None', 'None', 'lunch')"""
        return sql_request(sql)

    @staticmethod
    def end_lunch(full_name):
        sql = f"""UPDATE lunches SET end_time = '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 
                        state = 'work' WHERE state = 'lunch' AND full_name = '{full_name}';"""
        return sql_request(sql)

    @staticmethod
    def check_state(full_name):
        sql = f"""SELECT state FROM lunches WHERE full_name = '{full_name}';"""
        return sql_request(sql)
        
    @staticmethod
    def duration(full_name):
        sql = f"""SELECT start_time, end_time FROM lunches WHERE state = 'work' AND full_name = '{full_name}'"""
        response = sql_request(sql)

        duration = datetime.datetime.strptime(response[0][1], '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(response[0][0], '%Y-%m-%d %H:%M:%S')

        sql = f"""UPDATE lunches SET duration = '{duration}', state = 'finished' WHERE state = 'work' AND full_name = '{full_name}'"""
        response = sql_request(sql)


class SkufIncidents:
    sql_request("""CREATE TABLE if not exists skuf_inc_data (inc_number text)""")

    @staticmethod
    def add_inc(inc):
        sql = f"""INSERT INTO skuf_inc_data VALUES ('{inc}')"""
        return sql_request(sql)

    @staticmethod
    def delete_inc(inc):
        sql = f"DELETE FROM skuf_inc_data WHERE inc_number = '{inc}';"
        return sql_request(sql)

    @staticmethod
    def get_inc():
        sql = f"SELECT inc_number FROM skuf_inc_data"
        result = sql_request(sql)
        data = []
        if result:
            for inc in result:
                data.append(inc[0])         
        return data


class AnalyticsAutoFAQ:
    sql_request("""CREATE TABLE if not exists analytics_autofaq (channel_id text, conversation_id text, 
                    create_date text, added_date text, operator_id text, status text)""")

    @staticmethod
    def add_data(data):
        sql = f"""INSERT INTO analytics_autofaq VALUES ('{data[0]}', '{data[1]}', 
                    '{data[2]}', '{data[3]}', '{data[4]}', '{data[5]}')"""
        return sql_request(sql)

    @staticmethod
    def get_all_data(channel_id):
        sql = f"""SELECT * FROM analytics_autofaq WHERE channel_id = '{channel_id}';"""
        return sql_request(sql)

    @staticmethod
    def get_data_from_date(date):
        sql = f"""SELECT * FROM analytics_autofaq WHERE added_date = '{date}';"""
        return sql_request(sql)

    @staticmethod
    def get_conversation_id(channel_id):
        sql = f"""SELECT conversation_id FROM analytics_autofaq WHERE channel_id = '{channel_id}';"""
        return sql_request(sql)
    
    @staticmethod
    def get_all_conversation_ids():
        sql = f"""SELECT conversation_id FROM analytics_autofaq;"""
        return sql_request(sql)

    @staticmethod
    def get_status(conversation_id):
        sql = f"""SELECT status FROM analytics_autofaq WHERE conversation_id = '{conversation_id}';"""
        return sql_request(sql)

    @staticmethod
    def get_operators(conversation_id):
        sql = f"""SELECT operator_id FROM analytics_autofaq WHERE conversation_id = '{conversation_id}';"""
        return sql_request(sql)

    @staticmethod
    def update_operator(operator_id, conversation_id):
        sql = f"""UPDATE analytics_autofaq SET operator_id = '{operator_id}' WHERE conversation_id = '{conversation_id}';"""
        return sql_request(sql)

    @staticmethod
    def update_status(status, conversation_id):
        sql = f"""UPDATE analytics_autofaq SET status = '{status}' WHERE conversation_id = '{conversation_id}';"""
        return sql_request(sql)


class NTTM_ticket_info:
    sql_request("""CREATE TABLE if not exists nttm_ticket_info (id_record bigserial, 
                    id_ticket text, add_date text, update_date text, ticket_data jsonb, status text)""")

    @staticmethod
    def add_ticket_data(result, ticket, status):
        json_data = json.dumps(result)
        sql = f"""INSERT INTO nttm_ticket_info VALUES (DEFAULT, '{ticket}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 
                        'None', '{json_data}', '{status}')"""
        return sql_request(sql)

    @staticmethod
    def update_ticket_data(result, ticket, status):
        json_data = json.dumps(result)
        sql = f"""UPDATE nttm_ticket_info SET update_date = '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 
                        ticket_data = '{json_data}', status = '{status}' WHERE id_ticket = '{ticket}';"""
        return sql_request(sql)


class NTTM_dashboard_today:
    sql_request("""CREATE TABLE if not exists nttm_dashboard_today (id_record bigserial, 
                    add_date text, query_data jsonb, update_date text, change_data jsonb)""")
    
    @staticmethod
    def count_records():
        sql = f"""SELECT Count(*) FROM nttm_dashboard_today"""
        return sql_request(sql)

    @staticmethod
    def add_dashboard_data(result):
        json_data = json.dumps(result)
        sql = f"""INSERT INTO nttm_dashboard_today VALUES (DEFAULT, '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 
                        '{json_data}', 'None', 'None')"""
        return sql_request(sql)
    
    @staticmethod
    def update_dashboard_data(result, record):
        json_data = json.dumps(result)
        sql = f"""UPDATE nttm_dashboard_today SET update_date = '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 
                        change_data = '{json_data}' WHERE id_record = '{record}';"""
        return sql_request(sql)


class NTTM_dashboard_history:
    sql_request("""CREATE TABLE if not exists nttm_dashboard_history (id_record bigserial, 
                    date text, analytics_data jsonb)""")

    @staticmethod
    def add_dashboard_data(analytics):
        json_data = json.dumps(analytics)
        sql = f"""INSERT INTO nttm_dashboard_history VALUES (DEFAULT, '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{json_data}')"""
        return sql_request(sql)