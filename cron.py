import time
import json
import psycopg2
import requests
import datetime
import database
import threading
from log import logger
from schedule_job import schedule
from ws_server import services
import telegram


def event_duty(updater):
    users_data = database.sql_request("SELECT full_name, chat_id FROM users")
    duty_tomorrow = schedule.get_duty_tomorrow()
    # Составляем списки отсутствующих
    text_off = ''
    off_day_users = []

    for user_data in users_data:
        if not schedule.get_work_time_user(datetime.datetime.now() + datetime.timedelta(days=1), user_data[0]):
            off_day_users.append('<code>' + user_data[0] + '</code>')
    if len(off_day_users) > 0:
        text_off += f"\nОтсутствующие сотрудники завтра: \n" + '\n'.join(off_day_users)

    text = f'Напоминание: <code>завтра дежурство с 10 до 19</code>\n' \
           f'{text_off}'

    for user_event in duty_tomorrow:
        for user_tg in users_data:
            if user_event == user_tg[0]:
                if user_tg[1] is not None:
                    if database.Settings.get_working_mode() == 'prod':
                        try:
                            updater.bot.send_message(chat_id=user_tg[1], text=text, parse_mode='HTML')
                        except telegram.error.Unauthorized:
                            logger.debug(f"Не удалось отправить уведомление о дежурстве, бот в блокировке у {user_event}")
                            continue
                    else:
                        logger.debug(f'send tg: {text}')
                    logger.info(f"Пользователю {user_event} отправлено уведомление о дежурстве")
                else:
                    logger.warning(f"У пользователя {user_event} отсутствует chat_id в БД")


def rotation_tables():
    """Авто-очистка БД"""

    connect = psycopg2.connect(dbname=False, user=False, 
                    password=False, host=False, port=False)
    cursor = connect.cursor()
    cursor.execute("SELECT Count(*) FROM calls;")
    count = cursor.fetchall()[0][0]

    if count > 1000000:
        cursor.execute("SELECT session_id FROM calls ORDER BY end_time")
        delete = cursor.fetchall()[:500000]
        query = "DELETE from calls WHERE session_id = (%s)"
        cursor.executemany(query, delete)
        connect.commit()

    connect.close()


def keepalive_rest(disconnect):
    import warnings
    url_rest = False
    headers = False

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for step in range(1, 4):
            try:
                response = requests.get(url_rest, headers=headers, verify=False, timeout=4)
                if response.status_code == 200:
                    break
                else:
                    if not disconnect:
                        logger.error(f"REST API не отвечает (попытка {step}): {err}")
            except Exception as err:
                if not disconnect:
                    logger.warning(f"REST API не отвечает (попытка {step}): {err}")
        else:
            if not disconnect:
                logger.error(f"REST API не отвечает: {err}")
            disconnect = True

    if response.status_code == 200 and disconnect:
        disconnect = False
        database.Calls.fix_calls()
        logger.warning(f"REST API восстановлен. Вызовы в БД скорректированы.")

    return disconnect


def keepalive_svetofor(updater, disconnect_vmsp, unregister, unregistered_sip_users):
    chat_id = database.Settings.get_chat_id_svetofor()
    fail_counter = 0

    connection = services.vmsp.check_client()
    if connection:
        if disconnect_vmsp == True:
            text = (f"✅ Доступ к хосту восстановлен ✅ \n\nХост: p241i07aster01 \nIP-адрес: (192.168.236.27)")
            updater.bot.send_message(chat_id=chat_id, text=text)
            disconnect_vmsp = False

        for step in range(1, 5): 
            try:
                keepalive_response = services.vmsp.keep_alive()
                if keepalive_response['status_code'] == 200:
                    result = keepalive_response['result']
                    for peer, user_status in result.items():
                        if unregister:
                            unregister = False
                            break
                        for key, value in user_status.items():
                            if not value == 'Registered':
                                if not unregister and fail_counter < 3:
                                    retry_register = services.vmsp.send_register(peer)
                                    fail_counter += 1
                                    unregister = True
                                    break
                                else:
                                    if not key in unregistered_sip_users and not value == 'Registered':
                                        unregistered_sip_users.append(key)
                                        text = (f"🔥У пользователя ({key}) отсутствует регистрация! Проверьте подключение пира к Asterisk🔥 \
                                                \n\nПир: {peer} \nХост: p241i07aster01 \nIP-адрес: (192.168.236.27)")
                                        updater.bot.send_message(chat_id=chat_id, text=text)
                            else: 
                                if key in unregistered_sip_users:
                                    unregistered_sip_users.remove(key)
                                    text = (f"✅ Регистрация пользователя ({key}) прошла успешно ✅ \
                                            \n\nПир: {peer} \nХост: p241i07aster01 \nIP-адрес: (192.168.236.27)")
                                    updater.bot.send_message(chat_id=chat_id, text=text)
                    if not fail_counter:
                        break
                else:
                    if not disconnect_vmsp:
                        logger.warning(f"🔥Хост не отвечает🔥 \n\nХост: p241i07aster01 \
                                \nIP-адрес: (192.168.236.27)")  
            except Exception as err:
                if not disconnect_vmsp: 
                    logger.warning(f"🔥Произошла ошибка при запросе хоста🔥 \n\nХост: p241i07aster01 \
                            \nIP-адрес: (192.168.236.27) \nОшибка: {err}")
    else:
        text = (f"🔥Отсутствует подключение к хосту🔥 \n\nХост: p241i07aster01 \nIP-адрес: (192.168.236.27)")
        updater.bot.send_message(chat_id=chat_id, text=text)
        disconnect_vmsp = True

    return disconnect_vmsp, unregister, unregistered_sip_users


class WatchDog:
    _states = {}
    _lock = threading.Lock()
    _trigger = None
    run = True

    @classmethod
    def push(cls, name, rate_sleep):
        """
        :param name: название сервиса
        :param rate_sleep: нормальное количество секунд простоя
        """
        with cls._lock:
            cls._states[name] = {"time": time.time(), "rate": rate_sleep}

    @classmethod
    def delete(cls, name):
        with cls._lock:
            cls._states.pop(name, None)

    @classmethod
    def check(cls):
        warn_t = int(database.Settings.get_setting("alarm_watchdog")) * 60
        curr_t = time.time()
        with cls._lock:
            if cls._states:
                res = {k: True if curr_t - (v['time'] + v["rate"]) < warn_t else False for k, v in cls._states.items()}
                if False in res.values():
                    cls._event(res)

    @classmethod
    def _event(cls, result):
        limit_t = int(database.Settings.get_setting("limit_time_watchdog")) * 60
        if not cls._trigger or time.time() - cls._trigger >= limit_t:
            cls._trigger = time.time()
            text = '\n'.join(map(lambda x: f"{x[0]}: {x[1]}", result.items()))
            logger.warning(text)

    @classmethod
    def get_text(cls):
        if cls._states:
            message = []
            warn_t = int(database.Settings.get_setting("alarm_watchdog")) * 60
            curr_t = time.time()

            for k ,v in cls._states.items():
                state = "🟢" if curr_t - (v['time'] + v["rate"]) < warn_t else "🔴"
                tz = datetime.timezone(datetime.timedelta(hours=3))
                hum_time = datetime.datetime.fromtimestamp(v['time'], tz).strftime('%m-%d %H:%M')
                message.append(f"{state} {k}:  {hum_time}")
            return '\n'.join(message)


def start(updater):
    dates_event = [datetime.datetime.now() - datetime.timedelta(seconds=1) for _ in range(4)]
    disconnect_rest, disconnect_vmsp, unregister = False, False, False
    unregistered_sip_users = list()

    while True:
        now = datetime.datetime.now()
        today_date = now.strftime('%Y-%m-%d')

        try:
            # Проверяем жив ли rest api
            if now >= dates_event[0]:
                disconnect_rest = keepalive_rest(disconnect_rest)
                dates_event[0] = now + datetime.timedelta(seconds=2)

            # Контролируем размер таблицы calls
            if now >= dates_event[1]:
                rotation_tables()
                dates_event[1] = now + datetime.timedelta(days=1)

            # Уведомление о дежурстве со списком отсутствующих
            if now >= dates_event[2]:
                event_duty(updater)
                dates_event[2] = (now + datetime.timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0)

            # Проверяем жив ли сервер с Asterisk (Светофор аварий)
            if now >= dates_event[3]:
                time.sleep(5)
                disconnect_vmsp, unregister, unregistered_sip_users = keepalive_svetofor(updater, disconnect_vmsp, unregister, unregistered_sip_users) 
                dates_event[3] = now + datetime.timedelta(seconds=60)

            WatchDog.push("cron", 0)

            # watchdog: проверяет состояние подсистем
            WatchDog.check()

        except Exception as err:
            logger.error(f"Ошибка в cron: {err}")
            continue

        time.sleep(1)