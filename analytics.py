from log import logger
from ws_server import services
from schedule_job import schedule
from cron import WatchDog
from requests.auth import HTTPBasicAuth
import requests
import datetime
import time
import os
import database
import re
import threading
import copy
import warnings
import pandas as pd
import telegram
import json


class AnalyticsAll:
    def __init__(self, updater):
        self.nttm = AnalyticsNTTM(updater)
        self.calls = AnalyticsCalls()
        self.skuf = AnalyticsSKUF()
        self._send = updater.bot.send_message
        self._run = True
        threading.Thread(target=self._core, daemon=True).start()

    def statistics_realtime(self):
        def sorted_work_time(data):
            if '🟢 ' in data[0]:
                return int(data[1])
            else:
                return 1000

        def check_work_time(user):
            if schedule_analyst == None:
                return None
            if schedule_analyst.get(user):
                if schedule_analyst[user][0] < now < schedule_analyst[user][1]:
                    return True
            elif schedule_analyst.get(user) is None:
                return None
            return False

        def mes_format_text(data):
            step1 = max(list(map(lambda x: len(x[0]), data))) + 1
            step2 = max(list(map(lambda x: len(x[1]), data))) + 3
            step3 = max(list(map(lambda x: len(x[2]), data))) + 3
            step4 = max(list(map(lambda x: len(x[3]), data))) + 3
            column_name = f"⏳ ФИО{' ' * (step1 - 6)}▶   ️↪  ⏸     📞️\n\n"
            table = ''

            for user in data:
                name, inc_wk, inc_rd, inc_wt, calls = user
                spaces1 = step1 - len(name)
                spaces2 = step2 - len(inc_wk)
                spaces3 = step3 - len(inc_rd)
                spaces4 = step4 - len(inc_wt)

                row = f"{name}{' ' * spaces1}{inc_wk}{' ' * spaces2}{inc_rd}{' ' * spaces3}{inc_wt}{' ' * spaces4}{calls}\n"
                table += row

            return '<pre>' + column_name + table + '</pre>'

        calls_data = self.calls.analise_end_calls().get_data()
        nttm_data = self.nttm.get_statistics_direction()

        if nttm_data['update_time']:
            update_time_nttm = nttm_data['update_time']
        else:
            update_time_nttm = 'Данные отсутствуют'
        mes = f"<code>Обновление данных NTTM:</code> {update_time_nttm}\n"
        mes += f"<code>ТТ в работе:</code> {nttm_data['inc_work']}\n"
        mes += f"<code>Новых ТТ в очереди:</code> {nttm_data['inc_queue']}\n"
        mes += f"<code>Возвратов в очереди:</code> {nttm_data['inc_returned']}\n"
        mes += f"<code>Приостановленных:</code> {nttm_data['inc_wait']}\n"
        mes += f"<code>Возвратов без исполнителя:</code> {nttm_data['inc_return_without_executor']}\n"
        mes += f"<code>Приостановленных без исполнителя:</code> {nttm_data['inc_wait_without_executor']}\n\n"

        schedule_analyst = schedule.get_schedule_today()
        now = datetime.datetime.now()
        data_anal = []
        for user_tg in calls_data:
            temp_list = []
            full_name_orig = calls_data[user_tg]['full_name']
            result_check_work_time = check_work_time(full_name_orig)
            if result_check_work_time:
                work_time_user = '🟢 '
            elif result_check_work_time == None:
                work_time_user = '❔ '
            else:
                work_time_user = '💤 '

            if calls_data[user_tg]['full_name'] == None or calls_data[user_tg]['full_name'] == "None":
                full_name = f"@{user_tg}"
                full_name_orig = 'Нет ФИО'
            else:
                full_name_list = full_name_orig.split(' ')[:2]
                full_name_list[1] = full_name_list[1][:1] + '.'
                full_name = ' '.join(full_name_list)

            temp_list.append(f"{work_time_user}{full_name} ")
            temp_list.append(str(nttm_data['users'][full_name_orig]['inc_work']))
            temp_list.append(str(nttm_data['users'][full_name_orig]['inc_returned']))
            temp_list.append(str(nttm_data['users'][full_name_orig]['inc_wait']))

            if calls_data[user_tg]['state'] == 'connected':
                temp_list.append("💬")
            elif calls_data[user_tg]['state'] == 'disconnected':
                warning_call = ''
                if calls_data[user_tg]['date_end'] < (now - datetime.timedelta(hours=2)) \
                        and result_check_work_time != False:
                    warning_call = '❗️'
                temp_list.append(f"🗿 {calls_data[user_tg]['date_end'].strftime('%d %H:%M')} {warning_call}")
            elif calls_data[user_tg]['state'] is None:
                if calls_data[user_tg]['username_vats'] in [None, 'None']:
                    temp_list.append("⚙ нет логина")
                else:
                    temp_list.append("⛔ нет выз.")
            data_anal.append(temp_list)

        data_anal = sorted(data_anal, key=sorted_work_time)
        mes += mes_format_text(data_anal)
        return mes

    @property
    def run(self):
        return self._run

    @run.setter
    def run(self, value):
        if value is True:
            self._run = True
            threading.Thread(target=self._core, daemon=True).start()
        elif value is False:
            self._run = False
        else:
            raise ValueError('Run parameter can only be True or False')

    def _analise(self, absence_minutes):
        all_users = database.Users.get_custom_params('username_tg', 'full_name', 'analytics')

        triggers = {}
        for user in all_users:
            if user[2] and schedule.check_work_time_user(datetime.datetime.now(), user[1]):
                triggers[user[0]] = {'full_name': user[1], 'analise': {}}

        nttm_data = self.nttm.get_last_activity()
        skuf_data = self.skuf.get_statistics()
        calls_end = self.calls.analise_end_calls().get_data()

        now = datetime.datetime.now()
        event_time = datetime.timedelta(minutes=absence_minutes)

        for user_tg in triggers:
            full_name = triggers[user_tg]['full_name']

            if not self.nttm.state:
                triggers[user_tg]['analise']['nttm'] = None
            elif now - nttm_data[full_name] > event_time:
                triggers[user_tg]['analise']['nttm'] = False
            else:
                triggers[user_tg]['analise']['nttm'] = True

            if not self.skuf.state:
                triggers[user_tg]['analise']['skuf'] = None
            elif now - skuf_data[user_tg] > event_time:
                triggers[user_tg]['analise']['skuf'] = False
            else:
                triggers[user_tg]['analise']['skuf'] = True

            if calls_end[user_tg]['state'] == 'connected':
                triggers[user_tg]['analise']['calls'] = True
            elif calls_end[user_tg]['state'] == 'disconnected':
                if now - calls_end[user_tg]['date_end'] > event_time:
                    triggers[user_tg]['analise']['calls'] = False
                else:
                    triggers[user_tg]['analise']['calls'] = True
            else:
                triggers[user_tg]['analise']['calls'] = None

        return triggers

    def _event(self, triggers, absence_minutes):
        rows = []

        for user_tg in triggers:
            all_analise = list(triggers[user_tg]['analise'].values())
            if None not in all_analise[:2] and not any(all_analise):
                full_name_list = triggers[user_tg]['full_name'].split(' ')[:2]
                full_name_list[1] = full_name_list[1][:1] + '.'
                full_name = ' '.join(full_name_list)

                rows.append(f"{full_name} нет активности\n")

        if rows:
            systems = ', '.join(list(triggers[user_tg]['analise']))
            message = f"Активность отслеживается в {systems}.\n" \
                      f"Триггер при отсутствии активности более {absence_minutes} мин.\n\n"
            message += ''.join(rows)

            if database.Settings.get_working_mode() == 'prod':
                self._send(chat_id=database.Settings.get_chat_id_analyst(), text=message)
            else:
                logger.debug(message)

    def _core(self):
        while self._run:
            if 4 < datetime.datetime.now().hour < 19:
                WatchDog.push(self.__class__.__name__, 900)
                absence_minutes = database.Settings.get_absence_minutes()
                triggers = self._analise(absence_minutes)
                self._event(triggers, absence_minutes)
                time.sleep(900)
            else:
                WatchDog.push(self.__class__.__name__, 1800)
                time.sleep(1800)


class AnalyticsAutoFAQ:
    def __init__(self):
        self.calls = AnalyticsCalls()
        self._login = False
        self._password = False
        self._service_af = False

    def statistics_realtime(self):
        def autorization():
            url = 'https://autofaq.rt-dc.ru/api/ext/v2/login'
            autorize = requests.get(url=url, auth=HTTPBasicAuth(self._login, self._password))
            token = autorize.text
            return token
        
        def get_data(date):
            url = f'https://autofaq.rt-dc.ru/api/ext/v2/services/{self._service_af}/conversations'

            headers = {
                "Authorization":f"Bearer {autorization()}",
                "Content-Type":"application/json"
                }

            body = json.dumps({
                "tsFrom": f"{date}T00:00:00Z",
                "tsTo": f"{date}T23:59:59Z",
                "limit": 1000,
                "page": 1,
                "orderDirection": "Asc",
                "conversationStatusList": [
                    "OnOperator", "Active", "AssignedToOperator", "ClosedByBot", "ClosedByOperator"]
                })

            request = requests.get(url=url, headers=headers, data=body)

            if request.status_code == 200:
                return json.loads(request.text)
            else:
                return None

        def update_db_data(raw_data, today):
            appeals = raw_data["items"]
            db = database.AnalyticsAutoFAQ

            for appeal in appeals:
                channel_id = appeal["channel"]["id"]
                conversation_id = appeal["conversationId"]
                create_date = appeal["ts"]
                operator_id = appeal["stats"]["participatingOperators"]
                actually_status = appeal["stats"]["usedStatuses"]
 
                operator_id = appeal["stats"]["participatingOperators"][-1] if operator_id else 'None'
                actually_status = appeal["stats"]["usedStatuses"][-1] if actually_status else 'None'

                get_conversation_ids = db.get_all_conversation_ids()

                if get_conversation_ids:
                    all_session_ids = [sessions[0] for sessions in get_conversation_ids]
                    if not conversation_id in all_session_ids:
                        data = [channel_id, conversation_id, create_date, today, operator_id, actually_status]
                        db.add_data(data)
                    else:
                        operator = db.get_operators(conversation_id)
                        if not operator == operator_id:
                            db.update_operator(operator_id, conversation_id)

                        status = db.get_status(conversation_id)
                        if not status == actually_status:
                            actually_status = db.update_status(actually_status, conversation_id)
                else:    
                    data = [channel_id, conversation_id, create_date, today, operator_id, actually_status]
                    db.add_data(data)

        def analyze(today):
            data = database.AnalyticsAutoFAQ.get_data_from_date(today)

            states = [0 for _ in range(5)]
            status_operator = {}

            for items in data:
                if items[5] == 'Active':
                    states[0] += 1
                elif items[5] == 'AssignedToOperator':
                    states[1] += 1
                    operator = items[4]
                    status_operator[operator] = "AssignedToOperator"
                elif items[5] == 'OnOperator':
                    states[2] += 1
                elif items[5] == 'ClosedByBot':
                    states[3] += 1
                else:
                    states[4] += 1
                    operator = items[4]
                    status_operator[operator] = "ClosedByOperator"        
            return states, status_operator

        def sorted_work_time(data):
            if '🟢 ' in data[0]:
                return int(data[1])
            else:
                return 1000

        def check_work_time(user):
            if schedule_analyst == None:
                return None
            if schedule_analyst.get(user):
                if schedule_analyst[user][0] < now < schedule_analyst[user][1]:
                    return True
            elif schedule_analyst.get(user) is None:
                return None
            return False

        def mes_format_text(data):
            step1 = max(list(map(lambda x: len(x[0]), data))) + 1
            step2 = max(list(map(lambda x: len(x[1]), data))) + 3
            step3 = max(list(map(lambda x: len(x[2]), data))) + 3

            column_name = f"👨🏻‍💻 ФИО{' ' * (step1 - 6)}🔘   ️☑️      📞️\n\n"
            table = ''

            for user in data:
                name, in_work, closed, calls = user
                spaces1 = step1 - len(name)
                spaces2 = step2 - len(in_work)
                spaces3 = step3 - len(closed)

                row = f"{name}{' ' * spaces1}{in_work}{' ' * spaces2}{closed}{' ' * spaces3}{calls}\n"
                table += row
            return '<pre>' + column_name + table + '</pre>'

        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d')
        data_autofaq = get_data(today)
        
        if data_autofaq:
            update_time_autofaq = str(now)[:-7]
            update_db_data(data_autofaq, today)
            states, operators_status = analyze(today)
        else:
            update_time_autofaq = 'Данные отсутствуют'
            states = [0 for _ in range(5)]

        mes = f"<code>Обновление данных из AutoFAQ:</code> {update_time_autofaq}\n\n"
        mes += f"<code>Статистика на {today}</code>\n\n"
        mes += f"<code>Запросов в работе у бота:</code> {str(states[0])}\n"
        mes += f"<code>Запросов в работе у операторов:</code> {str(states[1])}\n"
        mes += f"<code>Запросов в очереди на операторов:</code> {str(states[2])}\n"
        mes += f"<code>Закрыто запросов ботом:</code> {str(states[3])}\n"
        mes += f"<code>Закрыто запросов оператором:</code> {str(states[4])}\n\n"
        
        calls_data = self.calls.analise_end_calls().get_data()
        schedule_analyst = schedule.get_schedule_today()

        data_analytics = []
        for user_tg in calls_data:
            autofaq_id = database.Users.get_user_autofaq_id(user_tg)

            if autofaq_id:
                temp_list = []
                full_name_orig = calls_data[user_tg]['full_name']
                result_check_work_time = check_work_time(full_name_orig)

                if result_check_work_time:
                    work_time_user = '🟢 '
                elif result_check_work_time == None:
                    work_time_user = '❔ '
                else:
                    work_time_user = '💤 '

                if calls_data[user_tg]['full_name'] == None or calls_data[user_tg]['full_name'] == "None":
                    full_name = f"@{user_tg}"
                    full_name_orig = 'Нет ФИО'
                else:
                    full_name_list = full_name_orig.split(' ')[:2]
                    full_name_list[1] = full_name_list[1][:1] + '.'
                    full_name = ' '.join(full_name_list)

                AssignedToOperator, ClosedByOperator = 0, 0
                for key, value in operators_status.items():
                    if autofaq_id == key:
                        if value == "AssignedToOperator":
                            AssignedToOperator += 1
                        elif value == "ClosedByOperator":
                            ClosedByOperator += 1

                temp_list.append(f"{work_time_user}{full_name} ")
                temp_list.append(str(AssignedToOperator)) 
                temp_list.append(str(ClosedByOperator)) 

                if calls_data[user_tg]['state'] == 'connected':
                    temp_list.append("💬 в разговоре ")
                elif calls_data[user_tg]['state'] == 'disconnected':
                    warning_call = ''
                    if calls_data[user_tg]['date_end'] < (now - datetime.timedelta(hours=2)) \
                            and result_check_work_time != False:
                        warning_call = '❗️'
                    temp_list.append(f"🗿 {calls_data[user_tg]['date_end'].strftime('%d.%m %H:%M')} {warning_call}")
                elif calls_data[user_tg]['state'] is None:
                    if calls_data[user_tg]['username_vats'] in [None, 'None']:
                        temp_list.append("⚙ нет логина")
                    else:
                        temp_list.append("⛔ нет выз.")
                data_analytics.append(temp_list)

        data_analytics = sorted(data_analytics, key=sorted_work_time)
        mes += mes_format_text(data_analytics)
        return mes


class AnaliseEndCalls:
    """Класс анализа для AnalyticsCalls"""

    def __init__(self):
        self._data = self._create_raw_data()
        self._completed = False

    def _create_raw_data(self):
        users_data = database.Users.get_custom_params('username_tg', 'full_name', 'username_vats',
                                                                'analytics')
        data = {}
        for user in users_data:
            if user[3]:
                if user[2] is None:
                    us_vats = None
                else:
                    us_vats = user[2].split(';')

                data.update({user[0]: {'username_vats': us_vats, 'full_name': user[1],
                                       'state': None, 'date_end': None}})
        return data

    def analise_call(self, call):
        """Возвращает True если данные собраны, False если готов анализировать дальше"""
        if self._completed:
            return True

        type_call = call[0]
        state_call = call[1]

        if type_call == 'outbound':
            username_vats = call[5][4:].split('@')[0]
        elif type_call == 'incoming':
            username_vats = call[6][4:].split('@')[0]
        else:
            return False

        for us_tg in self._data:
            if self._data[us_tg]['state'] is None:
                if self._data[us_tg]['username_vats'] is not None:
                    if username_vats in self._data[us_tg]['username_vats']:
                        if state_call == 'disconnected':
                            self._data[us_tg]['date_end'] = datetime.datetime.strptime(call[4], '%Y-%m-%d %H:%M:%S.%f')
                            self._data[us_tg]['state'] = state_call
                        elif state_call == 'connected':
                            self._data[us_tg]['state'] = state_call

        if all([us['state'] for us in self._data.values() if us['username_vats']]):
            self._completed = True
            return True
        else:
            return False

    def get_data(self):
        return self._data


class AnaliseCountCalls:
    """Класс анализа для AnalyticsCalls"""

    def __init__(self, date_start, date_end):
        self._date_start = date_start
        self._date_end = date_end
        self._data = self._create_raw_data()
        self._completed = False

    def _create_raw_data(self):
        users_data = database.Users.get_custom_params('username_tg', 'full_name', 'username_vats',
                                                                'analytics')
        data = {'counter_calls': 0, 'counter_minutes': datetime.timedelta(seconds=0),
                'average_calls': 0, 'average_minutes': datetime.timedelta(seconds=0),
                'users': {}, 'users_no_account': []}
        for user in users_data:
            if not user[3]:
                continue

            if user[1]:
                us_work_time = schedule.get_work_time_user(self._date_start, user[1])
                if not us_work_time or us_work_time[0] > self._date_end or us_work_time[1] < self._date_start:
                    continue

            if user[2] is None:
                data['users_no_account'].append(user[0])
            else:
                data['users'].update({user[0]: {'username_vats': user[2].split(';'), 'full_name': user[1],
                                                'calls': 0, 'minutes': datetime.timedelta(seconds=0)}})
        return data

    def calc_average(self):
        total_users = len(self._data['users'])
        if total_users:
            self._data['average_calls'] = self._data['counter_calls'] // total_users
            self._data['average_minutes'] = self._data['counter_minutes'] / total_users

    def analise_call(self, call):
        """Возвращает True если данные собраны, False если готов анализировать дальше"""
        if self._completed:
            return True

        state_call = call[1]

        if state_call != 'disconnected':
            return False

        end_time_call = datetime.datetime.strptime(call[4], '%Y-%m-%d %H:%M:%S.%f')
        call_time = end_time_call - datetime.datetime.strptime(call[3], '%Y-%m-%d %H:%M:%S.%f')
        type_call = call[0]

        # Берем вызовы из диапазона
        if end_time_call > self._date_end:
            return False
        if end_time_call < self._date_start:
            self.calc_average()
            self._completed = True
            return True

        if type_call == 'outbound':
            username_vats = call[5][4:].split('@')[0]
        elif type_call == 'incoming':
            username_vats = call[6][4:].split('@')[0]
        else:
            return False

        for user_tg in self._data['users']:
            if username_vats in self._data['users'][user_tg]['username_vats']:
                # Общая статистика
                self._data['counter_calls'] += 1
                self._data['counter_minutes'] += call_time

                # Индивидуальная статистика
                self._data['users'][user_tg]['calls'] += 1
                self._data['users'][user_tg]['minutes'] += call_time
        return False

    def get_csv(self):
        file_csv = 'name;calls;minutes\r\n'.encode('cp1251')
        for us_tg in self._data['users']:
            if self._data['users'][us_tg]['full_name'] is None:
                full_name = f'@{us_tg}'
            else:
                full_name = self._data['users'][us_tg]['full_name']

            if self._data['users'][us_tg]['calls'] == 0:
                calls = 0
                minutes = 0
            else:
                calls = self._data['users'][us_tg]['calls']
                minutes = str(self._data['users'][us_tg]['minutes'])[:-7]
            file_csv += f"{full_name};{calls};{minutes}\r\n".encode('cp1251')
        return file_csv

    def get_data(self):
        self.calc_average()
        return self._data


class AnalyticsCalls:
    def analise_calls(self, hours):
        now = datetime.datetime.now()
        start_time = now - datetime.timedelta(hours=hours)

        end_calls = AnaliseEndCalls()
        count_calls = AnaliseCountCalls(date_start=start_time, date_end=now)
        analyzers = [end_calls, count_calls]

        self.core_analise(analyzers)
        return analyzers

    def analise_end_calls(self):
        end_calls = AnaliseEndCalls()
        self.core_analise([end_calls])
        return end_calls

    def analise_count_calls(self, hours):
        now = datetime.datetime.now()
        start_time = now - datetime.timedelta(hours=hours)

        count_calls = AnaliseCountCalls(date_start=start_time, date_end=now)
        self.core_analise([count_calls])
        return count_calls

    @staticmethod
    def core_analise(analyzers):
        progress = [False] * len(analyzers)

        for call in database.Calls.get_calls_bd():
            for i, analyzer in enumerate(analyzers):
                completed = analyzer.analise_call(call)
                progress[i] = completed

            if all(progress):
                break


class AnalyticsNTTM:
    def __init__(self, updater):
        self._users_analise = self._create_raw_data_direction(None)
        now = datetime.datetime.now()
        self._last_activity = {user[0]: now for user in database.Users.get_custom_params('full_name')}

        # происходит ли анализ
        self.state = False

        self._task_states = {'inc_wait': [], 'inc_returned': []}
        self._new_tt = []
        self._sender = updater.bot.send_message
        self._lock = threading.RLock()
        threading.Thread(target=self._core, daemon=True).start()

    def _send(self, chat_id, text, user=None):
        try:
            self._sender(chat_id=chat_id, text=text)
            logger.debug(f'Отправлено уведомление из Аналитики НТТМ пользователю {user}: {text}')
        except telegram.error.Unauthorized:
            logger.debug(f"Не удалось отправить пользователю {user} уведомление, бот в блокировке.")

    def _create_raw_data_direction(self, update_time):
        users = {}
        for user in database.Users.get_users_info():
            users.update({user[3]: {'inc_work': 0, 'inc_returned': 0, 'inc_wait': 0,
                                    'chat_id': user[5]}})

        return {'users': users, 'inc_queue': 0, 'inc_returned': 0,
                'inc_work': 0, 'inc_wait': 0, 'update_time': update_time,
                'inc_return_without_executor': 0, 'inc_wait_without_executor': 0}

    def _supervision(self):
        """Следит когда последний раз в работе был ТТ"""
        up_time = self._users_analise['update_time']
        for full_name in self._users_analise['users']:
            if self._users_analise['users'][full_name]['inc_work'] > 0:
                self._last_activity[full_name] = up_time

    def search_executor(self, task, users):
            if task['coordinatorUser'] in users:
                return task['coordinatorUser']
            else:
                # Вычисляем какого исполнителя возврат если не указан координатор
                response_ = services.nttm.get_inc(task['ticketId'])
                if response_['status_code'] != 200:
                    logger.error(f"Не удалось получить инцидент {response_}")
                    return

                tasks_ = response_['result']['tasks']
                tasks_.reverse()

                for task_inc in tasks_:
                    if task_inc['taskExecutorDTO']['execUnitName'] in ('ДЭФИР ЛЦК ВАТС', 'ДЭФИР ЛЦК Бесплатный вызов 2.0'):
                        if task_inc['taskExecutorDTO']['executorName'] in users:
                            return task_inc['taskExecutorDTO']['executorName']

    def _parser_direction(self, tasks):
        groups = ['ДЭФИР ЛЦК Бесплатный вызов 2.0', 'ДЭФИР ЛЦК ВАТС']
        statistics = self._create_raw_data_direction(datetime.datetime.now())
        new_tt = []

        for task in tasks:
            # учитываем инциденты только на наших группах
            if task['execUnit'] in groups:
                
                # Возвраты
                if task['taskStatus'] == 'В очереди' and task['taskType'] == 'Проверка':
                    if task['coordinatorUser'] in statistics['users']:

                        executor_tt = task['coordinatorUser']
                        response = services.nttm.get_inc(task['ticketId'])

                        if response['status_code'] != 200:
                            logger.error(f"Не удалось получить инцидент {response}")
                            continue

                        inc = response['result']

                        # Если инцидент возвращается на проверку от нас же (перед закрытием)
                        if inc['tasks'][-2]['taskExecutorDTO']['executorName'] == executor_tt:
                            continue

                        # Общий счетчик инцидентов возвратов
                        statistics['inc_returned'] += 1
                        
                        executor_return = self.search_executor(task, statistics['users'])
                        if not executor_return:
                            logger.info(f"Не удалось найти исполнителя: {executor_return}")
                            continue
                        user_work_time = schedule.check_work_time_user(datetime.datetime.now(), executor_return)

                        if executor_return and user_work_time:
                            statistics['users'][executor_return]['inc_returned'] += 1
                        else:
                            statistics['inc_return_without_executor'] += 1

                        # Отправка уведомления
                        if task['taskNumber'] not in self._task_states['inc_returned']:
                            try:
                                date_kdSla = datetime.datetime.strptime(task['kd'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            except Exception:
                                date_kdSla = datetime.datetime.strptime(task['kd'], '%Y-%m-%dT%H:%M:%SZ')

                            left_kdSla = (date_kdSla + datetime.timedelta(hours=3)) - datetime.datetime.now()

                            text = f"📢 Уведомление\n" \
                                f"Вернулся ТТ {task['ticketId']} от смежного подразделения\n" \
                                f"Домен: {task['domain']}\n"

                            if left_kdSla < datetime.timedelta(0):
                                text += "SLA просрочен"
                            else:
                                text += f"SLA истекает через: {str(left_kdSla)[:-7]}"

                            if database.Settings.get_working_mode() == 'prod':
                                if executor_return and user_work_time:
                                    self._send(chat_id=statistics['users'][executor_return]['chat_id'],
                                            text=text, user=executor_return)
                                else:
                                    self._send(chat_id=database.Settings.get_chat_id_duty(), text=text, user='duty')
                            else:
                                logger.debug(f'send event nttm: {text}')

                            self._task_states['inc_returned'].append(task['taskNumber'])
                    else:
                        # Общий счетчик инцидентов возвратов
                        statistics['inc_returned'] += 1

                        executor_return = self.search_executor(task, statistics['users'])
                        if not executor_return:
                            logger.info(f"Не удалось найти исполнителя: {executor_return}")
                            continue
                        user_work_time = schedule.check_work_time_user(datetime.datetime.now(), executor_return)

                        if executor_return and user_work_time:
                            statistics['users'][executor_return]['inc_returned'] += 1
                        else:
                            statistics['inc_return_without_executor'] += 1

                        # Отправка уведомления
                        if task['taskNumber'] not in self._task_states['inc_returned']:
                            try:
                                date_kdSla = datetime.datetime.strptime(task['kd'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            except Exception:
                                date_kdSla = datetime.datetime.strptime(task['kd'], '%Y-%m-%dT%H:%M:%SZ')

                            left_kdSla = (date_kdSla + datetime.timedelta(hours=3)) - datetime.datetime.now()

                            text = f"📢 Уведомление\n" \
                                f"Вернулся ТТ {task['ticketId']} от смежного подразделения\n" \
                                f"Домен: {task['domain']}\n"

                            if left_kdSla < datetime.timedelta(0):
                                text += "SLA просрочен"
                            else:
                                text += f"SLA истекает через: {str(left_kdSla)[:-7]}"

                            if database.Settings.get_working_mode() == 'prod':
                                if executor_return and user_work_time:
                                    self._send(chat_id=statistics['users'][executor_return]['chat_id'],
                                            text=text, user=executor_return)
                                else:
                                    self._send(chat_id=database.Settings.get_chat_id_duty(), text=text, user='duty')
                            else:
                                logger.debug(f'send event nttm: {text}')

                            self._task_states['inc_returned'].append(task['taskNumber'])

                # Новые
                elif task['taskStatus'] == 'В очереди':
                    statistics['inc_queue'] += 1
                    new_tt.append(task)

                # В работе
                elif task['taskStatus'] == 'В работе':
                    # Общий счетчик инцидентов в работе
                    statistics['inc_work'] += 1

                    # Счетчик в работе по каждому сотруднику
                    if task['executor'] in statistics['users']:
                        statistics['users'][task['executor']]['inc_work'] += 1

            # В приостановке
            elif task['taskStatus'] == 'В очереди' and task['execUnit'] == 'Клиент' and task['taskType'] == 'Ожидание':

                if task['coordinatorUser'] in statistics['users']:
                    executor_wait = task['coordinatorUser']
                else:
                    # Вычисляем какого исполнителя возврат если не указан координатор
                    response = services.nttm.get_inc(task['ticketId'])
                    if response['status_code'] != 200:
                        logger.error(f"Не удалось получить инцидент {response}")
                        continue

                    inc = response['result']
                    executor_wait = inc['tasks'][-2]['taskExecutorDTO']['executorName']

                if executor_wait in statistics['users']:
                    user_work_time = schedule.check_work_time_user(datetime.datetime.now(), executor_wait)
                    statistics['inc_wait'] += 1
                    if user_work_time:
                        statistics['users'][executor_wait]['inc_wait'] += 1
                    else:
                        statistics['inc_wait_without_executor'] += 1

                    # Проверка был ли ранее event
                    if task['taskNumber'] not in self._task_states['inc_wait']:
                        # Через сколько вернется
                        left_wait = task['tillWaitingExit'].split(':')
                        left_wait = datetime.timedelta(hours=int(left_wait[0]), minutes=int(left_wait[1]))
                        alarm_time = datetime.timedelta(minutes=database.Settings.get_alarm_wait_inc())

                        # Уведомление о приостановке
                        if left_wait < alarm_time:
                            # Когда сгорит
                            try:
                                date_kdsla = datetime.datetime.strptime(task['kd'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            except Exception:
                                date_kdsla = datetime.datetime.strptime(task['kd'], '%Y-%m-%dT%H:%M:%SZ')
                            date_kdsla = (date_kdsla + datetime.timedelta(hours=3)) - datetime.datetime.now()

                            text = f"📢 Уведомление\n" \
                                   f"Через {task['tillWaitingExit']} вернется ТТ " \
                                   f"{task['ticketId']} из приостановки\n" \
                                   f"Домен: {task['domain']}\n"

                            if date_kdsla < datetime.timedelta(0):
                                text += "SLA просрочен"
                            else:
                                text += f"SLA истекает через: {str(date_kdsla)[:-7]}"

                            if user_work_time:
                                self._send(chat_id=statistics['users'][executor_wait]['chat_id'], text=text,
                                           user=executor_wait)
                            else:
                                self._send(chat_id=database.Settings.get_chat_id_duty(), text=text, user='duty')
                                statistics['inc_wait_without_executor'] += 1

                            self._task_states['inc_wait'].append(task['taskNumber'])

        self._new_tt = new_tt
        return statistics

    @staticmethod
    def _parser_vendor(tasks):
        pattern_vendor = re.compile(r'\d{15,17}')
        pattern_comment = re.compile(r"\n|\t|&nbsp|;|<.{,5}>")
        statistics = {'update_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'), 'tickets': {}}

        for task in tasks:

            # Тикеты на Cветец (Вендор)
            if task['execUnit'] == 'Вендор (ДЭФИР)' and task['taskStatus'] == 'В очереди' and \
                    task['taskType'] == 'Ожидание провайдера':
                response = services.nttm.get_inc(task['ticketId'])
                if response['status_code'] != 200:
                    logger.error(f"Не удалось получить инцидент {response}")
                    continue

                tasks_ = response['result']['tasks']
                tasks_.reverse()

                for ind, task_with_inc in enumerate(tasks_):
                    if task_with_inc['taskExecutorDTO'][
                        'execUnitName'] == 'ДЭФИР Эксплуатация платформ' and \
                            task_with_inc['typeName'] == 'Решение':

                        if 'closeComment' in task_with_inc.keys():
                            search_ticket = pattern_vendor.search(task_with_inc['closeComment'])

                        if search_ticket:
                            ticket = search_ticket.group()
                            comment = tasks_[ind + 1]['closeComment']
                            format_comment = pattern_comment.sub(" ", comment)
                            description = f"INC {task_with_inc['troubleTicketId']}: {format_comment}"

                            if ticket in statistics['tickets']:
                                if len(statistics['tickets'][ticket]['description']) < 2:
                                    statistics['tickets'][ticket]['description'].append(description)
                                statistics['tickets'][ticket]['inc'].append(str(task_with_inc['troubleTicketId']))
                            else:
                                statistics['tickets'][ticket] = {'description': [description],
                                                                 'inc': [str(task_with_inc[
                                                                                 'troubleTicketId'])]}
                        break
        return statistics

    def _client_query_vendor(self, dashboard):
        statistics = self._create_raw_data_direction(datetime.datetime.now())
        for task in dashboard:
            # Тикеты на Cветец (Вендор)
            if task['execUnit'] == 'Вендор (ДЭФИР)' and task['taskStatus'] == 'В очереди' and \
                    task['taskType'] == 'Ожидание провайдера':
                response = services.nttm.get_inc(task['ticketId'])
                if response['status_code'] != 200:
                    logger.error(f"Не удалось получить инцидент {response}")
                    continue
                
                tasks = response['result']['tasks']
                if tasks[-1]["typeName"] == "Запрос клиента":
                    if not tasks[-1]["taskExecutorDTO"]["execUnitName"] in ("ДЭФИР ЛЦК ВАТС", "ДЭФИР Дежурная смена ОТТ"):
                        if tasks[-1].get('closeComment'):
                            client_comment = tasks[-1]['closeComment'].replace('<p>', '').replace('</p>', '\n').replace('&nbsp;', '')
                            executor_return = self.search_executor(task, statistics['users'])
                            if not executor_return:
                                logger.info(f"Не удалось найти исполнителя: {executor_return}")
                                continue

                            user_work_time = schedule.check_work_time_user(datetime.datetime.now(), executor_return)

                            if executor_return and user_work_time:
                                statistics['users'][executor_return]['inc_returned'] += 1
                            else:
                                statistics['inc_return_without_executor'] += 1

                            # Отправка уведомления
                            if task['taskNumber'] not in self._task_states['inc_returned']:
                                text = f"📢 Уведомление\n" \
                                    f"В ТТ {task['ticketId']} новый запрос от клиента!\n" \
                                    f"Домен: {task['domain']}\n" \
                                    f"Комментарий: {client_comment}"

                                if database.Settings.get_working_mode() == 'prod':
                                    if executor_return and user_work_time:
                                        self._send(chat_id=statistics['users'][executor_return]['chat_id'],
                                                text=text, user=executor_return)
                                    else:
                                        self._send(chat_id=database.Settings.get_chat_id_duty(), text=text, user='duty')
                                else:
                                    logger.debug(f'send event nttm: {text}')
                                
                                self._task_states['inc_returned'].append(task['taskNumber'])

    def _core(self):
        # Создание потоков (от старой версии)
        def create_threads(parser, tasks, raw_data):
            num_work_tasks = 30
            th_list = []
            sum_tasks = len(tasks)
            if sum_tasks // num_work_tasks > 0:
                num_th = sum_tasks // num_work_tasks
                if sum_tasks % num_work_tasks > 0:
                    num_th += 1
            else:
                num_th = 1
            start_ = 0
            end_ = num_work_tasks
            for _ in range(0, num_th):
                pars_thread = threading.Thread(target=parser, args=[tasks[start_:end_], raw_data], daemon=True)
                pars_thread.start()
                th_list.append(pars_thread)
                start_ += num_work_tasks
                end_ += num_work_tasks

            for th in th_list:
                th.join()

        clear_inc_event = False
        start_vendor = True

        logger.info("Аналитика NTTM запущена")

        while True:
            try:
                WatchDog.push(self.__class__.__name__, 0)

                # Проверяем подключение к НТТМ
                if services.nttm.check_client():
                    self.state = True
                else:
                    self.state = False
                    time.sleep(1)
                    continue

                now = datetime.datetime.now()

                # Запуск аналитики подразделения
                if 4 < now.hour < 19:
                    clear_inc_event, start_vendor = True, True
                    response = services.nttm.get_tasks(database.Settings.get_nttm_filter_direction())
                    if response['status_code'] == 200:
                        result = self._parser_direction(response['result'])
                        with self._lock:
                            self._users_analise = result
                            self._supervision()
                    else:
                        continue

                # Запуск аналитики тикетов на вендоре (на наличие новых запросов от клиента)
                if 4 < now.hour < 19:
                    response = services.nttm.get_tasks(database.Settings.get_nttm_filter_vendor())
                    if response['status_code'] == 200:
                        result = self._client_query_vendor(response['result'])
                    else:
                        continue

                # Запуск аналитики тикетов на Светце
                elif 20 < now.hour < 23 and start_vendor:
                    response = services.nttm.get_tasks(database.Settings.get_nttm_filter_vendor())
                    if response['status_code'] == 200:
                        tickets_vendor = self._parser_vendor(response['result'])
                        database.Data.set_tickets_vendor(tickets_vendor)
                        start_vendor = False
                    else:
                        continue

                # Очистка данных о уведомлениях
                elif clear_inc_event:
                    for event_list in self._task_states:
                        self._task_states[event_list].clear()
                else:
                    self.state = False
                    time.sleep(600)

            except Exception as err:
                logger.error(err, exc_info=True)
                time.sleep(30)

    @property
    def new_tt(self):
        with self._lock:
            return self._new_tt.copy()

    @new_tt.setter
    def new_tt(self, value):
        with self._lock:
            self._new_tt = value

    def get_statistics_direction(self):
        with self._lock:
            return copy.copy(self._users_analise)

    def get_last_activity(self):
        with self._lock:
            return copy.copy(self._last_activity)

    def arrears_report(self, file_name):
        def analise(tt):
            analise_info = {}
            # ! SLA хранится в минутах
            tt_sla = int(tt['ola']['ksSla'])
            count_sla = 0

            if tt['status'] == 'В работе':
                if tt['tasks'][-1]['typeName'] == 'Ожидание':
                    analise_info['Результат'] = 'ТТ в приостановке'
                else:
                    analise_info['Результат'] = 'ТТ в работе'

            elif tt['status'] == 'Закрыт':
                for task in tt['tasks']:
                    if task['typeName'] not in ('Ожидание', 'Запрос клиента'):

                        # Конвертация времени в datetime
                        time_create = datetime.datetime.strptime(task['createTs'][0:19], "%Y-%m-%dT%H:%M:%S")
                        time_close = datetime.datetime.strptime(task['completionDate'][0:19], "%Y-%m-%dT%H:%M:%S")

                        time_work_on_request = (time_close - time_create).total_seconds() / 60
                        count_sla = count_sla + time_work_on_request

                        if count_sla >= tt_sla:
                            time_assign = datetime.datetime.strptime(task['assignmentDate'][0:19], "%Y-%m-%dT%H:%M:%S")
                            group = task['taskExecutorDTO']['execUnitName']

                            # Парсим последний коммент
                            if task.get('taskComments') and len(task['taskComments'][-1]['comment']) > 0:
                                end_comment = task['taskComments'][-1]['comment']
                                parasitic_data = ("<p>", "</p>", "&nbsp;", "<br>", "<strong>",
                                                  "</strong>", "&gt;", "&lt;")

                                for old in parasitic_data:
                                    end_comment = end_comment.replace(old, ' ')
                            else:
                                end_comment = "Комментарий отсутствует"

                            analise_info.update({'Результат': f"SLA превышен",
                                                 'Просрочено на группе': group,
                                                 'Номер запроса': task['taskNumber'],
                                                 'Время создания запроса': time_create.strftime("%Y-%m-%dT%H:%M:%S"),
                                                 'Время принятия в работу': time_assign.strftime("%Y-%m-%dT%H:%M:%S"),
                                                 'Время закрытия запроса': time_close.strftime("%Y-%m-%dT%H:%M:%S"),
                                                 'Время реакции': time_assign - time_create,
                                                 'Время решения': time_close - time_assign,
                                                 'Время всё': time_close - time_create,
                                                 'Последний комментарий (task)': end_comment})

                            if group == 'Вендор (ДЭФИР)' and task.get('foreignTicketId'):
                                # Номер тикета на вендоре
                                analise_info["ТТ на вендоре"] = f"{task['foreignTicketId']}" 
                                # Проверка что за вендор
                                analise_info["Наименование вендора"] = f"{task['providerName']}" 

                            break
                else:
                    analise_info['Результат'] = 'SLA не превышен'
            else:
                logger.info(f"В анализе найден неизвестный статус ТТ{tt['id']} {tt['status']}")

            text = '\n'.join(map(lambda x: f"{x[0]}: {x[1]}", analise_info.items()))
            
            return text, analise_info

        def search_responsible(result):
            if result.get('Просрочено на группе') == 'ДЭФИР 2ЛТП':
                return 'ОЭП'
            elif result.get('Просрочено на группе') == 'Вендор (ДЭФИР)' and result.get('Наименование вендора') == 'СВЕТЕЦ ТЕХНОЛОДЖИ':
                return 'ОЭП'
            elif result.get('Просрочено на группе') == 'Вендор (ДЭФИР)' and result.get('Наименование вендора') != 'СВЕТЕЦ ТЕХНОЛОДЖИ':
                return 'Вендор (проверить вручную!)'
            elif result.get('Просрочено на группе') == 'ДЭФИР Дежурная смена ОТТ':
                return 'ОТТ'
            elif result.get('Просрочено на группе') == 'ДЭФИР ЛЦК ВАТС':
                return 'ЛЦК'
            elif result.get('Просрочено на группе'):
                return 'МРФ'
            else:
                return result['Результат']

        report = pd.read_excel(file_name)
        os.remove(file_name)

        for index, row in report.iterrows():
            number_tt = row['№ ТТ']

            try:
                response = services.nttm.get_inc(number_tt)
                if response['status_code'] != 200:
                    response = services.nttm.get_inc(number_tt)
                    if response['status_code'] != 200:
                        logger.error(f'Не удалось получить данные по инциденту: {response["result"]}')
                        return False

                tt = response['result']
                text, res_analise = analise(tt)
                responsible = search_responsible(res_analise)
                
                report.loc[index, "ЗО"] = responsible
                report.loc[index, "Коммент"] = text

            except Exception as err:
                logger.error(f"Ошибка при парсинге отчета просрочек: {err}")
                report.loc[index, "Коммент"] = "При парсинге произошла ошибка. Необходимо проверить самостоятельно."

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=UserWarning)

            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                report.to_excel(writer, sheet_name='Sheet1', index=False)
                worksheet = writer.sheets['Sheet1']
                cell_format = writer.book.add_format({'text_wrap': True})
                worksheet.set_column("B:B", 40, cell_format)
                worksheet.set_column("C:C", 70, cell_format)
                writer.save()

        with open(file_name, 'rb') as file:
            file = file.read()
        os.remove(file_name)
        return file


class AnalyticsSKUF:
    def __init__(self):
        self._users_data = {}
        for user in database.Users.get_custom_params('username_tg'):
            self._users_data[user[0]] = datetime.datetime.now()
        self._lock = threading.RLock()

    @property
    def state(self):
        return services.nttm.check_client()

    def get_statistics(self):
        with self._lock:
            return copy.deepcopy(self._users_data)

    def accept_inc(self, username):
        with self._lock:
            if self._users_data.get(username):
                self._users_data[username] = datetime.datetime.now()

    def resolved_inc(self, username):
        with self._lock:
            if self._users_data.get(username):
                self._users_data[username] = datetime.datetime.now()
