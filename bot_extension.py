import datetime
from telegram.ext import *
from telegram import *
from schedule_job import schedule
from ws_server import services
from wiki import WIKI
from log import logger
from cron import WatchDog
import database
import threading
import time
import analytics
import json


updater = Updater(token=database.Settings.get_token(), use_context=True)
analyst = analytics.AnalyticsAll(updater)
autofaq_analyst = analytics.AnalyticsAutoFAQ(updater)


def start_analytics_autofaq(updater):
    date_event = datetime.datetime.now() - datetime.timedelta(seconds=1)

    time.sleep(5)

    while True:
        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d')
        yesterday = (now - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        try:
            if now >= date_event:
                SkufIncAutoFAQ._inc_request(updater, today, yesterday)
                date_event = now + datetime.timedelta(seconds=60)
        except Exception as err:
            logger.error(f"Ошибка при запросе INC AutoFAQ из СКУФ: {err}")
            continue
        
        time.sleep(30)


def temp_mes(update, context, text, time_kill=2, keyboard=None):
    if keyboard is None:
        answer_mes = context.bot.send_message(chat_id=update.effective_chat.id,
                                              text=text)
    else:
        answer_mes = context.bot.send_message(chat_id=update.effective_chat.id,
                                              text=text, reply_markup=keyboard)

    def killer_thread():
        time.sleep(time_kill)
        context.bot.delete_message(chat_id=update.effective_chat.id, message_id=answer_mes['message_id'])

    kill = threading.Thread(target=killer_thread, daemon=True)
    kill.start()


@database.auth_user(access='access_5')
def start(update, context):
    text = "Приветствую! Я бот менеджер услуги ВАТС.\nДля вызова меню используй команду /menu\n"
    update.message.reply_text(text=text)


def new_menu(update, context):
    username = update.effective_user.username
    update.message.delete()
    if database.checking_privileges(username, 'user'):
        if not database.Users.check_chat_id(username) and update.effective_chat.type == 'private':
            database.Users.set_user_chat_id(username, update.effective_chat.id)
    menu(update, context)


def general_callback_handler(update, context):
    username = update.effective_user.username
    query = update.callback_query.data
    handlers = (BugMenu, NttmMenu, SkufActionInc, AdminMenu, AnalyticsMenu, WikiMenu)
    logger.debug(f"user {username} use button {query}")

    if query == 'back_general_menu':
        menu(update, context, new_mes=False)
    elif query in ('closeMenu', 'close'):
        update.callback_query.message.delete()
    elif query == 'None':
        context.bot.answer_callback_query(update.callback_query.id, text="Ждите!")
    else:
        for handler in handlers:
            if handler.callback_handler(update, context):
                break
        else:
            menu(update, context, new_mes=False)
            context.bot.answer_callback_query(update.callback_query.id, text="Устаревшая кнопка. Попробуйте снова.")
    context.bot.answer_callback_query(update.callback_query.id, text="")


@database.auth_user(access='access_5')
def menu(update, context, new_mes=True):
    username = update.effective_user.username

    if database.checking_privileges(username, 'user'):
        if not database.Users.check_chat_id(username) and update.effective_chat.type == 'private':
            database.Users.set_user_chat_id(username, update.effective_chat.id)

    keyboard = []
    if database.checking_privileges(username, 'admin'):
        row = [InlineKeyboardButton("Админ панель", callback_data='admin_menu')]
        keyboard.append(row)

    row = [InlineKeyboardButton("bug🕷", callback_data='bug_menu')]
    if database.checking_privileges(username, 'access_4'):
        row.append(InlineKeyboardButton("Создать INC", callback_data='inc_create'))
    keyboard.append(row)

    row = [InlineKeyboardButton("WIKI", callback_data='wiki_menu')]
    if database.checking_privileges(username, 'access_4'):
        row.append(InlineKeyboardButton("NTTM", callback_data='nttm_menu'))
    keyboard.append(row)

    if database.checking_privileges(username, 'root'):
        keyboard.append([InlineKeyboardButton("⚙️Настройки", callback_data='settings_menu')])
    keyboard.append([InlineKeyboardButton("✖️Закрыть меню", callback_data='closeMenu')])

    if new_mes:
        context.bot.send_message(chat_id=update.effective_chat.id, text='Меню',
                                 reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        update.callback_query.message.edit_text(text='Меню',
                                                reply_markup=InlineKeyboardMarkup(keyboard))


class WikiMenu:
    _dialog_search = 1
    _temp_state = {}

    @classmethod
    @database.auth_user(access='access_5')
    def _wiki_menu(cls, update, context):
        username = update.effective_user.username
        cls._temp_state.pop(username, None)
        keyboard = [[InlineKeyboardButton("Поиск в вики", callback_data='wiki_search')],
                    [InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')]]
        update.callback_query.message.edit_text(text='Меню', reply_markup=InlineKeyboardMarkup(keyboard))

    @classmethod
    def _wiki_search(cls, update, context):
        username = update.effective_user.username
        if update.callback_query is not None:
            query = update.callback_query.data
            if query == 'wiki_search':
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Вернуться назад", callback_data='back')]])
                cls._temp_state[username] = {'mes_id': update.callback_query.message.message_id, 'search': None}
                update.callback_query.message.edit_text(text='Напишите что вы хотите найти.', reply_markup=keyboard)
                return cls._dialog_search

            elif query == 'next':
                update.callback_query.message.edit_text(**cls._temp_state[username]['search'].next())
            elif query == 'prev':
                update.callback_query.message.edit_text(**cls._temp_state[username]['search'].prev())
            elif query == 'back':
                cls._wiki_menu(update, context)
                return ConversationHandler.END

        elif update.message is not None:
            update.message.delete()
            if cls._temp_state[username]['search']:
                return

            text = update.message.text
            search = WIKI.search(text)
            if search == 500:
                temp_mes(update, context, 'При попытке поиска возникла ошибка!', time_kill=7)
            elif search:
                cls._temp_state[username]['search'] = search
                context.bot.edit_message_text(chat_id=update.effective_chat.id,
                                              message_id=cls._temp_state[username]['mes_id'], **search.next())
            else:
                text = 'По вашему запросу не удалось найти информацию. Напишите новый запрос или вернитесь назад.'
                temp_mes(update, context, text, time_kill=7)

    @classmethod
    def callback_handler(cls, update, context):
        query = update.callback_query.data

        if query == 'wiki_menu':
            cls._wiki_menu(update, context)
        else:
            return False
        return True

    @classmethod
    def get_handler(cls):
        handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cls._wiki_search, pattern='wiki_search')],
            states={cls._dialog_search: [MessageHandler(Filters.text, cls._wiki_search)]},
            fallbacks=[CallbackQueryHandler(cls._wiki_search)],
            conversation_timeout=120)
        return handler


class BugMenu:
    @staticmethod
    @database.auth_user(access='access_5')
    def _bug_menu(update, context):
        keyboard = [[InlineKeyboardButton("TT на вендорах", callback_data='bug_get_file_vendor')],
                    [InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')]]
        update.callback_query.message.edit_text(text='Меню', reply_markup=InlineKeyboardMarkup(keyboard))

    @classmethod
    @database.auth_user(access='access_5')
    def _bug_get_file_vendor(cls, update, context):
        query = update.callback_query.data

        if query == 'bug_get_file_vendor':
            keyboard, row = [], []

            row.append(InlineKeyboardButton("Windows", callback_data='bug_get_file_vendor:cp1251'))
            row.append(InlineKeyboardButton("UNIX(macOS, IOS, Linux)", callback_data='bug_get_file_vendor:utf8'))
            keyboard.append(row)

            row = []
            row.append(InlineKeyboardButton("🔙 Вернуться назад", callback_data='bug_menu'))
            keyboard.append(row)

            update.callback_query.message.edit_text(
                text='Выбирите тип вашего устройства. Это необходимо что бы выбрать правильную кодировку и не увидеть кракозябры.',
                reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            codec = query.split(':')[1]
            update_date, file = database.Data.get_tickets_vendor(codec)
            if file:
                context.bot.send_document(chat_id=update.effective_chat.id, document=file,
                                          filename='tickets_vendor.csv',
                                          caption=f"Последнее обновление данных: {update_date}")
                cls._bug_menu(update, context)
            else:
                context.bot.answer_callback_query(update.callback_query.id,
                                                  text="Данные отсутствуют, попробуйте позже.")

    @classmethod
    def callback_handler(cls, update, context):
        query = update.callback_query.data

        if query == 'bug_menu':
            cls._bug_menu(update, context)
            return True
        elif query[:19] == 'bug_get_file_vendor':
            cls._bug_get_file_vendor(update, context)
            return True
        return False


class AdminMenu:
    _dialog_arrears = 1
    _temp = {}

    @staticmethod
    @database.auth_user(access='admin')
    def _admin_menu(update, context):
        username = update.effective_user.username
        full_name = database.Users.get_fullname_by_username_tg(username)
        keyboard = []
        row = [InlineKeyboardButton("Аналитика", callback_data='analytics_menu')]

        if database.checking_privileges(username, 'root'):
            on_lunch = False
            keyboard.append([InlineKeyboardButton("Состояние бота", callback_data='bot_states')])
            row.append(InlineKeyboardButton("Редактировать УЗ", callback_data='edit_users_menu'))

            for element in database.Lunches.check_state(full_name):
                if element[0] == 'lunch':
                    on_lunch = True

            if on_lunch:
                keyboard.append([InlineKeyboardButton("🍽 Вернуться с обеда", callback_data='return_from_lunch')])
            else:
                keyboard.append([InlineKeyboardButton("🍽 Уйти на обед", callback_data='break_for_lunch')])
                
        keyboard.append(row)
        keyboard.append([InlineKeyboardButton("Аналитика AutoFAQ", callback_data='autofaq_analytics_menu')])
        keyboard.append([InlineKeyboardButton("Проверить просрочки", callback_data='arrears_report_menu')])
        keyboard.append([InlineKeyboardButton("Создать ГР на следующий месяц",
                                              callback_data='create_next_month_schedule')])

        keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')])

        update.callback_query.message.edit_text(text='Меню',
                                                reply_markup=InlineKeyboardMarkup(keyboard))

    @staticmethod
    def _states(update, context):
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Вернуться назад", callback_data='admin_menu')]])
        skuf = "🟢" if services.skuf.check_client() else "🔴"
        nttm = "🟢" if services.nttm.check_client() else "🔴"
        vmsp = "🟢" if services.vmsp.check_client() else "🔴"

        call_date_str = next(database.Calls.get_calls_bd())[3]
        call_date = datetime.datetime.strptime(call_date_str, '%Y-%m-%d %H:%M:%S.%f')
        now = datetime.datetime.now()
        warn_t = int(database.Settings.get_setting("alarm_watchdog"))
        call = "🟢" if now - call_date < datetime.timedelta(minutes=warn_t) else "🔴"
        call_mes = call_date.strftime('%m-%d %H:%M')

        other = WatchDog.get_text()
        text = f"{skuf} Модуль СКУФ\n{nttm} Модуль NTTM\n{vmsp} Модуль VMSP\n{call} Последний вызов: {call_mes}\n{other}"
        update.callback_query.message.edit_text(text=text, reply_markup=keyboard)

    @classmethod
    def _create_schedule(cls, update, context):
        def th(update, context):
            update.callback_query.message.edit_text(text='Работаю над созданием графика...', parse_mode='HTML')
            result = schedule.create_next_month()
            if result == 1:
                text = "График на следующий месяц создан"
            elif result == 2:
                text = "График на следующий месяц уже существует"
            elif result == 3:
                text = "При создании возникла ошибка"

            update.callback_query.message.edit_text(text=text, parse_mode='HTML')

        threading.Thread(target=th, args=(update, context), daemon=True).start()

    @classmethod
    def _break_for_lunch(cls, update, context):
        username = update.effective_user.username
        query = update.callback_query.data

        def th(update, context):
            update.callback_query.message.edit_text(text='Отмечаю уход на обед...', parse_mode='HTML')
            full_name = database.Users.get_fullname_by_username_tg(username)

            if database.Lunches.start_lunch(full_name):
                update.callback_query.message.edit_text(text="Уход на обед отмечен!", parse_mode='HTML')
            else:
                update.callback_query.message.edit_text(text="Не удалось отметить уход на обед. Попробуйте снова", parse_mode='HTML')
                logger.error('Не удалось отметить уход на обед')

        threading.Thread(target=th, args=(update, context), daemon=True).start()

    @classmethod
    def _return_from_lunch(cls, update, context):
        username = update.effective_user.username
        query = update.callback_query.data

        def th(update, context):
            update.callback_query.message.edit_text(text='Отмечаю возврат с обеда...', parse_mode='HTML')
            full_name = database.Users.get_fullname_by_username_tg(username)
            
            if database.Lunches.end_lunch(full_name):
                update.callback_query.message.edit_text(text="Возврат с обеда отмечен!", parse_mode='HTML')
                state = database.Lunches.check_state(full_name)

                for element in state:
                    if element[0] == 'work':
                        duration = database.Lunches.duration(full_name)
            else:
                update.callback_query.message.edit_text(text="Не удалось отметить возврат с обеда. Попробуйте снова", parse_mode='HTML')
                logger.error('Не удалось отметить возврат с обеда')

        threading.Thread(target=th, args=(update, context), daemon=True).start()

    @classmethod
    def arrears_report_menu(cls, update, context):
        username = update.effective_user.username

        if not services.nttm.check_client():
            context.bot.answer_callback_query(update.callback_query.id, text="Нет доступа к NTTM")
            return ConversationHandler.END

        if update.callback_query is not None:
            keyboard = [[InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')]]
            update.callback_query.message.edit_text(text='Отправьте файл в excel формате',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))
            cls._temp[username] = {'mes_id': update.callback_query.message.message_id}
            return cls._dialog_arrears
        elif update.message is not None and update.message.document is not None:
            get_file = update.message.document.get_file()
            file_name = update.message.document.file_name
            get_file.download(file_name)
            update.message.delete()
            context.bot.edit_message_text(message_id=cls._temp[username]['mes_id'], chat_id=update.effective_chat.id,
                                          text='Ожидайте. По завершению работы вышлю отчет.')

            def th(mes_id):
                file = analyst.nttm.arrears_report(file_name)
                if file:
                    context.bot.send_document(chat_id=update.effective_chat.id, document=file, filename=file_name,
                                              caption=f"Отчет готов")
                    context.bot.delete_message(chat_id=update.effective_chat.id, message_id=mes_id)
                else:
                    context.bot.send_message(chat_id=update.effective_chat.id,
                                             text='Не удалось сделать отчет. Проверьте логи.')

            threading.Thread(target=th, args=(cls._temp[username]['mes_id'],), daemon=True).start()
            cls._temp.pop(username, None)
            return ConversationHandler.END

    @classmethod
    def callback_handler(cls, update, context):
        username = update.effective_user.username
        query = update.callback_query.data
        cls._temp.pop(username, None)

        if query in ['admin_menu', 'edit_users_menu_back']:
            cls._admin_menu(update, context)
        elif query == 'create_next_month_schedule':
            cls._create_schedule(update, context)
        elif query == 'arrears_report_menu':
            return cls.arrears_report_menu(update, context)
        elif query == 'bot_states':
            cls._states(update, context)
        elif query == 'break_for_lunch':
            cls._break_for_lunch(update, context)
        elif query == 'return_from_lunch':
            cls._return_from_lunch(update, context)
        elif query == 'back_general_menu':
            general_callback_handler(update, context)
            return ConversationHandler.END
        else:
            return False
        return True

    @classmethod
    def get_handler(cls):
        handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cls.callback_handler, pattern='arrears_report_menu')],
            states={cls._dialog_arrears: [MessageHandler(Filters.document, cls.arrears_report_menu)]},
            fallbacks=[CallbackQueryHandler(cls.callback_handler)],
            conversation_timeout=120)
        return handler


class AnalyticsMenu:
    @staticmethod
    @database.auth_user(access='admin')
    def analytics_menu(update, context):
        keyboard, row = [], []

        if analyst.run:
            row.append(InlineKeyboardButton("❌ Выключить аналитику", callback_data='analytics_control'))
        else:
            row.append(InlineKeyboardButton("⭕ Включить аналитику", callback_data='analytics_control'))

        row.append(InlineKeyboardButton("В реальном времени", callback_data='analytics_realtime'))
        keyboard.append(row)

        row = []
        row.append(InlineKeyboardButton("Заказать отчет", callback_data='order_report_analyst'))
        row.append(InlineKeyboardButton("🔙 Вернуться назад", callback_data='admin_menu'))
        keyboard.append(row)

        update.callback_query.message.edit_text(text='Меню',
                                                reply_markup=InlineKeyboardMarkup(keyboard))

    @staticmethod
    @database.auth_user(access='admin')
    def autofaq_analytics_menu(update, context):
        keyboard, row = [], []

        row.append(InlineKeyboardButton("В реальном времени", callback_data='af_analytics_realtime'))
        keyboard.append(row)

        row = []
        row.append(InlineKeyboardButton("🔙 Вернуться назад", callback_data='admin_menu'))
        keyboard.append(row)

        update.callback_query.message.edit_text(text='Меню',
                                                reply_markup=InlineKeyboardMarkup(keyboard))

    @classmethod
    @database.auth_user(access='admin')
    def analytics_control(cls, update, context):
        analyst.run = False if analyst.run else True
        cls.analytics_menu(update, context)

    @staticmethod
    @database.auth_user(access='admin')
    def analytics_realtime(update, context):
        keyboard = []
        text = analyst.statistics_realtime()

        row = [InlineKeyboardButton("🔙 Вернуться назад", callback_data='analytics_menu')]
        keyboard.append(row)

        update.callback_query.message.edit_text(text=text, parse_mode="HTML",
                                                reply_markup=InlineKeyboardMarkup(keyboard))

    @staticmethod
    @database.auth_user(access='admin')
    def af_analytics_realtime(update, context):
        keyboard = []
        text = autofaq_analyst.statistics_realtime()

        row = [InlineKeyboardButton("🔙 Вернуться назад", callback_data='autofaq_analytics_menu')]
        keyboard.append([InlineKeyboardButton("Детальная аналитика", callback_data='af_analyst_detail_report')])
        keyboard.append(row)

        update.callback_query.message.edit_text(text=text, parse_mode="HTML",
                                                reply_markup=InlineKeyboardMarkup(keyboard))

    @staticmethod
    @database.auth_user(access='admin')
    def analyst_order_report(update, context):
        keyboard = []

        row = []
        row.append(InlineKeyboardButton("За час", callback_data='report_calls:1h'))
        row.append(InlineKeyboardButton("За 4 часа", callback_data='report_calls:4h'))
        keyboard.append(row)

        row = []
        row.append(InlineKeyboardButton("За 8 часов", callback_data='report_calls:8h'))
        row.append(InlineKeyboardButton("🔙 Вернуться назад", callback_data='analytics_menu'))
        keyboard.append(row)

        update.callback_query.message.edit_text(text='Меню',
                                                reply_markup=InlineKeyboardMarkup(keyboard))

    @staticmethod
    @database.auth_user(access='admin')
    def af_analyst_detail_report(update, context):
        query = update.callback_query.data

        file = analyst.calls.analise_count_calls(12).get_csv()

        context.bot.send_document(chat_id=update.effective_chat.id, document=file,
                                  filename='detail_analytics_af.csv')

    @staticmethod
    @database.auth_user(access='admin')
    def report_calls(update, context):
        query = update.callback_query.data

        if query == 'report_calls:1h':
            file = analyst.calls.analise_count_calls(1).get_csv()
        elif query == 'report_calls:4h':
            file = analyst.calls.analise_count_calls(4).get_csv()
        elif query == 'report_calls:8h':
            file = analyst.calls.analise_count_calls(8).get_csv()

        context.bot.send_document(chat_id=update.effective_chat.id, document=file,
                                  filename='analytics.csv')

    @classmethod
    def callback_handler(cls, update, context):
        query = update.callback_query.data

        if query == 'analytics_menu':
            cls.analytics_menu(update, context)
        elif query == 'autofaq_analytics_menu':
            cls.autofaq_analytics_menu(update, context)
        elif query == 'analytics_control':
            cls.analytics_control(update, context)
        elif query == 'af_analytics_control':
            cls.af_analytics_control(update, context)
        elif query == 'analytics_realtime':
            cls.analytics_realtime(update, context)
        elif query == 'af_analytics_realtime':
            cls.af_analytics_realtime(update, context)
        elif query == 'order_report_analyst':
            cls.analyst_order_report(update, context)
        elif query == 'af_analyst_detail_report':
            cls.af_analyst_detail_report(update, context)
        elif query[:12] == 'report_calls':
            cls.report_calls(update, context)
        else:
            return False
        return True


class EditUsersMenu:
    _edit_users_mes, _edit_users_callback = range(2)
    _temp_state = {}

    @classmethod
    @database.auth_user(access='root')
    def _edit_users_menu(cls, update, context):
        users_info = database.Users.get_users_info()
        username = update.effective_user.username
        cls._temp_state.pop(username, None)
        keyboard = []

        for user in users_info:
            keyboard.append([InlineKeyboardButton(f"{user[3]}", callback_data=f'edit_user:{user[0]}')])

        keyboard.append([InlineKeyboardButton("💡 Создать пользователя", callback_data='edit_users_create')])
        keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='edit_users_menu_back')])

        update.callback_query.message.edit_text(text='Меню', reply_markup=InlineKeyboardMarkup(keyboard))
        return cls._edit_users_callback

    @classmethod
    @database.auth_user(access='root')
    def _edit_users_create(cls, update, context):
        username = update.effective_user.username
        cls._temp_state.update({username: {'type_edit': 'create_user',
                                           'mesId': update.callback_query.message.message_id}})

        keyboard = [[InlineKeyboardButton("🔙 Вернуться назад", callback_data='edit_users_menu')]]
        update.callback_query.message.edit_text(
            text='Введите логин telegram нового пользователя, например tndrcloud',
            reply_markup=InlineKeyboardMarkup(keyboard))
        return cls._edit_users_mes

    @classmethod
    @database.auth_user(access='root')
    def _edit_user(cls, update, context):
        query = update.callback_query.data
        mod_user_tg = query.split(':')[1]
        users_info = database.Users.get_users_info()
        columns = database.Users.get_users_column_name()
        keyboard = []
        username = update.effective_user.username
        cls._temp_state.pop(username, None)

        for user in users_info:
            if user[0] == mod_user_tg:
                for param, column_name in zip(user, columns):
                    if param in [None, 'None']:
                        param = str(param) + ' ❗'
                    if column_name == 'analytics':
                        param = 'enabled' if param else 'disabled'
                    keyboard.append([InlineKeyboardButton(f"{column_name}: {param}",
                                                          callback_data=f"edit_user_param:{user[0]}:{column_name}")])
                break
        keyboard.append(
            [InlineKeyboardButton("🗑 Удалить пользователя",
                                  callback_data=f'edit_user_param:{user[0]}:delete_user')])
        keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='edit_users_menu')])

        update.callback_query.message.edit_text(text='Нажмите на нужный параметр для редактирования.',
                                                reply_markup=InlineKeyboardMarkup(keyboard))
        return cls._edit_users_callback

    @classmethod
    @database.auth_user(access='root')
    def _edit_user_param(cls, update, context):
        query = update.callback_query.data.split(':')
        username = update.effective_user.username

        def save_and_edit_keboard(text):
            keyboard = [[InlineKeyboardButton("🔙 Вернуться назад", callback_data=f'edit_user:{query[1]}')]]
            cls._temp_state.update({username: {'username_tg': query[1], 'type_edit': query[2],
                                               'mesId': update.callback_query.message.message_id}})
            update.callback_query.message.edit_text(text=text,
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

        if query[2] == 'username_tg':
            text = f'Напишите логин telegram, например tndrcloud'
            save_and_edit_keboard(text)
            return cls._edit_users_mes

        elif query[2] == 'chat_id':
            database.Users.del_user_chat_id(query[1])
            keyboard = [[InlineKeyboardButton("🔙 Вернуться назад", callback_data=f'edit_user:{query[1]}')]]
            update.callback_query.message.edit_text(
                text=f'Chat_id для {query[1]} удалён, при следующем взаимодействии'
                     f' с ботом в личных сообщениях параметр обновится.',
                reply_markup=InlineKeyboardMarkup(keyboard))

        elif query[2] == 'access':
            keyboard = []
            keyboard.append([InlineKeyboardButton("root", callback_data=f'access_edit:{query[1]}:root')])
            keyboard.append([InlineKeyboardButton("admin", callback_data=f'access_edit:{query[1]}:admin')])
            keyboard.append([InlineKeyboardButton("user", callback_data=f'access_edit:{query[1]}:user')])
            keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data=f'edit_user:{query[1]}')])
            update.callback_query.message.edit_text(text='Нажмите на нужный параметр access для редактирования.',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

        elif query[0] == 'access_edit':
            database.Users.set_user_access(query[1], query[2])
            keyboard = [[InlineKeyboardButton("🔙 Вернуться назад", callback_data=f'edit_user:{query[1]}')]]
            update.callback_query.message.edit_text(text=f'Права доступа для {query[1]} обновлены на {query[2]}',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

        elif query[2] == 'username_vats':
            username_vats = database.Users.get_user_username_vats(query[1])

            if username_vats[0] is None:
                username_vats = "None"
            else:
                username_vats = ";".join(username_vats)
            text = f'Напишите логин пользователя ВАТС, если необходимо указать ' \
                   f'несколько логинов используйте символ разделитель ";", ' \
                   f'например a.bardusov;bardusovalex;alexb\n' \
                   f'Если необходимо не получать отчеты, укажите None\n' \
                   f'Текущий список: {username_vats}'
            save_and_edit_keboard(text)
            return cls._edit_users_mes

        elif query[2] == 'full_name':
            text = f'Напишите ФИО, например Бардусов Александр Михайлович'
            save_and_edit_keboard(text)
            return cls._edit_users_mes

        elif query[2] == 'username_skuf':
            text = f'Напишите логин СКУФ, например aleksandr.bardusov'
            save_and_edit_keboard(text)
            return cls._edit_users_mes

        elif query[2] == 'autofaq_id':
            text = f'Напишите ID пользователя AutoFAQ'
            save_and_edit_keboard(text)
            return cls._edit_users_mes

        elif query[2] == 'analytics':
            database.Users.change_analytics(query[1])
            cls._edit_user(update, context)

        elif query[2] == 'delete_user':
            database.Users.del_user(query[1])
            context.bot.answer_callback_query(update.callback_query.id, text=f'Пользователь {query[1]} удален',
                                              show_alert=True)
            cls._edit_users_menu(update, context)

    @classmethod
    @database.auth_user(access='root')
    def _edit_user_mesHandler(cls, update, context):
        username = update.effective_user.username
        text = update.message.text

        def last_step(send_text, username_tg=None):
            if username_tg is None:
                username_tg = cls._temp_state[username]["username_tg"]
            keyboard = [[InlineKeyboardButton("🔙 Вернуться назад",
                                              callback_data=f'edit_user:{username_tg}')]]

            context.bot.edit_message_text(chat_id=update.effective_chat.id,
                                          message_id=cls._temp_state[username]['mesId'],
                                          text=send_text,
                                          reply_markup=InlineKeyboardMarkup(keyboard))
            update.message.delete()
            cls._temp_state.pop(username, None)

        if cls._temp_state[username]['type_edit'] == 'username_vats':
            send_text = f"Логин ВАТС для {cls._temp_state[username]['username_tg']} " \
                        f"обновлен на {text.split(';')}"
            database.Users.set_user_username_vats(cls._temp_state[username]['username_tg'], text)
            last_step(send_text)
            return cls._edit_users_callback

        elif cls._temp_state[username]['type_edit'] == 'username_tg':
            database.Users.set_user_username_tg(cls._temp_state[username]['username_tg'], text)
            send_text = f"Логин telegram для {cls._temp_state[username]['username_tg']} обновлен на {text}"
            last_step(send_text, text)
            return cls._edit_users_callback

        elif cls._temp_state[username]['type_edit'] == 'full_name':
            database.Users.set_user_full_name(cls._temp_state[username]['username_tg'], text)
            send_text = f"ФИО для {cls._temp_state[username]['username_tg']} обновлено на {text}"
            last_step(send_text)
            return cls._edit_users_callback

        elif cls._temp_state[username]['type_edit'] == 'username_skuf':
            database.Users.set_user_username_skuf(cls._temp_state[username]['username_tg'], text)
            send_text = f"Логин СКУФ для {cls._temp_state[username]['username_tg']} обновлен на {text}"
            last_step(send_text)
            return cls._edit_users_callback

        elif cls._temp_state[username]['type_edit'] == 'autofaq_id':
            database.Users.set_user_autofaq_id(cls._temp_state[username]['username_tg'], text)
            send_text = f"ID для пользователя {cls._temp_state[username]['username_tg']} обновлен на {text}"
            last_step(send_text)
            return cls._edit_users_callback

        elif cls._temp_state[username]['type_edit'] == 'create_user':
            database.Users.create_user_tg(text)
            keyboard = [
                [InlineKeyboardButton("Редактировать пользователя", callback_data=f'edit_user:{text}')],
                [InlineKeyboardButton("🔙 Вернуться назад", callback_data=f'edit_users_menu')]
            ]

            context.bot.edit_message_text(chat_id=update.effective_chat.id,
                                          message_id=cls._temp_state[username]['mesId'],
                                          text=f"Пользователь {text} создан",
                                          reply_markup=InlineKeyboardMarkup(keyboard))
            update.message.delete()
            cls._temp_state.pop(username, None)
            return cls._edit_users_callback

    @classmethod
    def get_handler(cls):
        handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cls._callback_handler, pattern='edit_users_menu')],
            states={cls._edit_users_mes: [MessageHandler(Filters.text, cls._edit_user_mesHandler)],
                    cls._edit_users_callback: [CallbackQueryHandler(cls._callback_handler)]},
            fallbacks=[CallbackQueryHandler(cls._callback_handler)],
            conversation_timeout=120
        )
        return handler

    @classmethod
    def _callback_handler(cls, update, context):
        query = update.callback_query.data

        if query == 'edit_users_menu_back':
            AdminMenu.callback_handler(update, context)
            return ConversationHandler.END
        elif query == 'edit_users_menu':
            return cls._edit_users_menu(update, context)
        elif query == 'edit_users_create':
            return cls._edit_users_create(update, context)
        elif query[:10] == 'edit_user:':
            return cls._edit_user(update, context)
        elif query[:16] == 'edit_user_param:' or query[:12] == 'access_edit:':
            return cls._edit_user_param(update, context)


class NttmMenu:
    st_add_si, st_select_si, st_join_si = range(3)
    temp_state = {}

    # Сделать диалог привязки к сетевому
    @classmethod
    def _nttm_menu(cls, update, context):
        username = update.effective_user.username
        keyboard = []

        if database.checking_privileges(username, 'admin'):
            keyboard.append([InlineKeyboardButton("Номера сетевых инцидентов", callback_data='nttm_change_si')])
        keyboard.append([InlineKeyboardButton("Привязать ТТ к сетевому", callback_data='join_ki_to_si')])
        keyboard.append([InlineKeyboardButton("Поиск проблем в ТТ", callback_data='nttm_search_problem_tt')])
        keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')])

        update.callback_query.message.edit_text(text='Выберите действие', reply_markup=InlineKeyboardMarkup(keyboard))

    @classmethod
    def inc_si_menu(cls, update, context, get_menu=False):
        keyboard = []
        for si in database.Data.get_si_inc():
            keyboard.append([InlineKeyboardButton(f"❌ {si}", callback_data=f"nttm_del_inc_si:{si}")])
        keyboard.append([InlineKeyboardButton("Добавить сетевой инцидент", callback_data='nttm_add_inc_si')])
        keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='nttm_menu')])
        params = dict(text='Выберите действие', reply_markup=InlineKeyboardMarkup(keyboard))

        if get_menu:
            return params
        else:
            update.callback_query.message.edit_text(**params)

    @classmethod
    def _add_inc_si_handler(cls, update, context):
        username = update.effective_user.username
        cls.temp_state.setdefault(username, {'mes_id': update.callback_query.message.message_id})
        text = 'Напишите номер сетевого инцидента'
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data='close')]])
        update.callback_query.message.edit_text(text=text, reply_markup=keyboard, parse_mode='HTML')
        return cls.st_add_si

    @classmethod
    def _add_inc_si_write(cls, update, context):
        username = update.effective_user.username
        update.message.delete()
        try:
            si = int(update.message.text)
        except ValueError:
            return temp_mes(update, context, "INC SI должен быть числом", time_kill=3)

        database.Data.add_si_inc(si)
        context.bot.edit_message_text(chat_id=update.effective_chat.id,
                                      message_id=cls.temp_state[username]['mes_id'],
                                      **cls.inc_si_menu(update, context, get_menu=True))
        cls.temp_state.pop(username, None)
        return ConversationHandler.END

    @classmethod
    def _del_inc_si(cls, update, context):
        si = update.callback_query.data.split(':')[1]
        database.Data.del_si_inc(si)
        cls.inc_si_menu(update, context)

    @classmethod
    def _close(cls, update, context):
        username = update.effective_user.username
        if update.message:
            update.message.delete()
        elif update.callback_query:
            update.callback_query.message.delete()
        cls.temp_state.pop(username, None)
        return ConversationHandler.END

    @classmethod
    def _join_ki_to_si_menu(cls, update, context):
        username = update.effective_user.username
        cls.temp_state.pop(username, None)

        # Проверяем подключен ли клиент в сокет
        if not services.nttm.check_client():
            context.bot.answer_callback_query(update.callback_query.id, text="Модуль NTTM не подключен!")
            logger.error('Попытка привязки тикета, клиент NTTM не подключен')
            return ConversationHandler.END

        si = database.Data.get_si_inc()
        if si:
            keyboard = [[InlineKeyboardButton(i, callback_data=i)] for i in database.Data.get_si_inc()]
            keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data='close')])
            keyboard = InlineKeyboardMarkup(keyboard)
            text = "Выберите к какому сетевому INC привязать тикет"
            update.callback_query.message.edit_text(text=text, reply_markup=keyboard, parse_mode='HTML')
            return cls.st_select_si
        else:
            text = "Не найдено сетевых inc. Обратитесь к администратору."
            context.bot.answer_callback_query(update.callback_query.id, text=text)
            return ConversationHandler.END

    @classmethod
    def _join_ki_to_si_handler(cls, update, context):
        if update.callback_query.data == "close":
            return cls._close(update, context)

        username = update.effective_user.username
        si = update.callback_query.data
        cls.temp_state[username] = {'si': si, 'mes_id': update.callback_query.message.message_id}
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data='close')]])
        update.callback_query.message.edit_text(text="Напишите номер одного тикета который необходимо привязать",
                                                reply_markup=keyboard)
        return cls.st_join_si

    @classmethod
    def _join_ki_to_si_performer(cls, update, context, thread_start=False):
        """функция запускает саму себя в отдельном потоке"""
        username = update.effective_user.username
        mes_id = cls.temp_state[username]['mes_id']
        si = cls.temp_state[username]['si']
        ki = update.message.text

        if thread_start is False:
            update.message.delete()
            text = f"Выполняю привязку {ki} к {si}. Ожидайте.."
            context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=mes_id, text=text)
            threading.Thread(target=cls._join_ki_to_si_performer, args=(update, context, True), daemon=True).start()
            return ConversationHandler.END
        else:
            cls.temp_state.pop(username, None)
            response = services.nttm.binding_network_inc(si, ki)

            if response['status_code'] == 200:
                text = f"✅ Привязка {ki} к {si} выполнена!"
            else:
                level = response['result']['level']
                step = response['result']['step']
                desc = response['result']['err']

                if response['status_code'] == 403:
                    text = f"⚠ {si}: {response['result']['err']}"
                    database.Data.del_si_inc(si)
                else:
                    text = f"⚠ Не удалось выполнить привязку {ki} к {si}! Пожалуйста привяжите самостоятельно."

                logger.error(f"Привязка к сетевому инциденту не удалась (lev: {level}, step: {step}): {desc}")
            context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=mes_id, text=text)

    @classmethod
    def callback_handler(cls, update, context):
        query = update.callback_query.data

        if query == 'nttm_menu':
            cls._nttm_menu(update, context)
        elif query == 'nttm_change_si':
            cls.inc_si_menu(update, context)
        elif 'nttm_del_inc_si' in query:
            cls._del_inc_si(update, context)
        elif query == 'nttm_search_problem_tt':
            context.bot.answer_callback_query(update.callback_query.id, text="Всё ещё в разработке :)")
        else:
            return False
        return True

    @classmethod
    def get_handler_add_si(cls):
        inc_create_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cls._add_inc_si_handler, pattern='nttm_add_inc_si')],
            states={cls.st_add_si: [MessageHandler(Filters.text, cls._add_inc_si_write)]},
            fallbacks=[CallbackQueryHandler(cls._close, pattern='close')],
            conversation_timeout=120)
        return inc_create_handler

    @classmethod
    def get_handler_link_ki(cls):
        inc_create_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cls._join_ki_to_si_menu, pattern='join_ki_to_si')],
            states={cls.st_select_si: [CallbackQueryHandler(cls._join_ki_to_si_handler)],
                    cls.st_join_si: [MessageHandler(Filters.text, cls._join_ki_to_si_performer)]},
            fallbacks=[CallbackQueryHandler(cls._close, pattern='close')],
            conversation_timeout=120)
        return inc_create_handler


class SkufActionInc:
    res_desc = {1: 'Консультация по ЛК', 2: 'Консультация по настройке оборудования', 3: 'Настройка  ЛК',
                4: 'Настройка оборудования', 5: 'Не ответил по кт', 6: 'Проблема передана в ТБ',
                7: 'Проблема передана в КБ'}

    @classmethod
    @database.auth_user(access='user')
    def _inc_accept(cls, update, context, thread_start=False):
        username = update.effective_user.username
        username_skuf = database.Users.get_username_skuf(username)

        if thread_start is False:
            params = dict(callback_query_id=update.callback_query.id, show_alert=False, text='')

            # Проверка chat_id пользователя
            if not database.Users.check_chat_id(username):
                params.update({'show_alert': True, 'text': f"Неизвестен ваш chat_id. Напиши боту в личку."})

            # Проверка наличия логина скуф в БД
            elif username_skuf == None:
                params.update({'show_alert': True, 'text': "Неизвестен ваш логин СКУФ. Обратитесь к @tndrcloud"})

            context.bot.answer_callback_query(**params)
            if not params['show_alert']:
                threading.Thread(target=cls._inc_accept, args=(update, context, True), daemon=True).start()
        else:
            analyst.skuf.accept_inc(username)
            query = update.callback_query.data.split(':')

            inc = query[1]
            full_name_list = database.Users.get_fullname_by_username_tg(username).split()
            full_name_skuf = ' '.join([full_name_list[0], full_name_list[1], full_name_list[2][:1]])

            task_accept = {'type': 'inc_accept', 'inc': inc, 'full_name_skuf': full_name_skuf,
                           'username_skuf': username_skuf}

            mes_orig = update.callback_query.message.text
            mes_accept = mes_orig.rstrip('#new_inc')
            keyboard = [[InlineKeyboardButton("Назначаю inc на исполнителя...", callback_data='None')]]
            update.callback_query.message.edit_text(text=mes_orig, parse_mode='HTML',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))
            response = services.skuf.create_task(task_accept)

            if response['status_code'] == 200:
                text = f'✅ {inc} принят в работу пользователем {full_name_skuf}.\n\n'
                text += mes_accept
                update.callback_query.message.edit_text(text=text, parse_mode='HTML')
                keyboard = [[InlineKeyboardButton("Перевести в решён 📝", callback_data=f'inc_resolved_menu:{inc}')]]
                chat_id = database.Users.get_user_chat_id(username)

                if chat_id:
                    context.bot.send_message(chat_id=chat_id, text=mes_accept, parse_mode='HTML',
                                             reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                keyboard = [[InlineKeyboardButton("Принять в работу 🔥", callback_data=update.callback_query.data)]]
                update.callback_query.message.edit_text(text=mes_orig, reply_markup=InlineKeyboardMarkup(keyboard))
                text = f'⛔ @{username} произошла ошибка принятия инцидента в работу. Err: {response["result"]}'
                context.bot.send_message(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
                logger.error(text)

    @classmethod
    def _inc_resolved_menu(cls, update, context):
        query = update.callback_query.data.split(':')
        inc = query[1]
        mes_text = update.callback_query.message.text
        keyboard = [[InlineKeyboardButton(v, callback_data=f"inc_resolved:{inc}:{k}")] for k, v in cls.res_desc.items()]
        update.callback_query.message.edit_text(text=mes_text, reply_markup=InlineKeyboardMarkup(keyboard))

    @classmethod
    def _inc_resolved(cls, update, context, thread_start=False):
        if thread_start is False:
            context.bot.answer_callback_query(update.callback_query.id, text='')
            threading.Thread(target=cls._inc_resolved, args=(update, context, True), daemon=True).start()
        else:
            username = update.effective_user.username
            analyst.skuf.resolved_inc(username)
            query = update.callback_query.data.split(':')
            inc = query[1]
            resolution = cls.res_desc[int(query[2])]
            task_resolved = {'type': 'inc_resolved', 'inc': inc, 'resolution': resolution}

            mes = update.callback_query.message.text
            keyboard = [[InlineKeyboardButton("Перевожу инцидент в решён..", callback_data='None')]]
            update.callback_query.message.edit_text(text=mes, parse_mode='HTML',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

            response = services.skuf.create_task(task_resolved)
            result = response['result']
            mes_inc_text = update.callback_query.message.text
            if response['status_code'] == 200:
                text = f"✅ Инцидент решён: {resolution}\n\n{mes_inc_text}"
            else:
                text = f'⛔ Инцидент решить не удалось. Просьба решить в СКУФ ⛔\n\n{mes_inc_text}'
                logger.error(text + str(result))
            update.callback_query.message.edit_text(text=text, parse_mode='HTML')

    @classmethod
    def callback_handler(cls, update, context):
        query = update.callback_query.data

        if 'inc_accept:' in query:
            cls._inc_accept(update, context)
        elif 'inc_resolved_menu:' in query:
            cls._inc_resolved_menu(update, context)
        elif 'inc_resolved:' in query:
            cls._inc_resolved(update, context)
        else:
            return False
        return True


class SkufCreateINC:
    st_initiator, st_description, st_review = range(3)
    initiators = ['Инсталлятор', 'Pre-sale', 'Covid']
    _temp_state = {}

    @classmethod
    @database.auth_user(access='access_4')
    def _select_initiator(cls, update, context):
        username = update.effective_user.username

        # Очищаем данные пользователя
        cls._temp_state.pop(username, None)

        # Проверяем подключен ли клиент в сокет
        if not services.skuf.check_client():
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text="Клиент для создания INC не подключен.")
            logger.error('Попытка создания инцидента, клиент инц не подключен')
            return ConversationHandler.END

        # Структура сообщения
        text = 'Выберите инициатора инцидента на клавиатуре'
        keyboard = [[InlineKeyboardButton(init, callback_data=init)] for init in cls.initiators]

        # Если меню вызвано кнопкой
        if update.callback_query:
            keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')])
            message_id = update.callback_query.message.message_id
            update.callback_query.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard))

        # Если меню вызвано командой
        else:
            update.effective_message.delete()
            keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data='close')])
            msg = context.bot.send_message(chat_id=update.effective_chat.id, text=text,
                                           reply_markup=InlineKeyboardMarkup(keyboard))
            message_id = msg['message_id']

        cls._temp_state.update({username: {'type': 'inc_create', 'initiator': None,
                                           'description': None, 'mesId': message_id}})
        return cls.st_initiator

    @classmethod
    def _msg_description(cls, update, context):
        username = update.effective_user.username
        cls._temp_state[username]['initiator'] = update.callback_query.data

        text = 'Напишите описание инцидента:\n<code>Имя\nКонтактный телефон\nОписание проблемы</code>'
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data='close')]]
        update.callback_query.message.edit_text(text=text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                parse_mode='HTML')
        return cls.st_description

    @classmethod
    def _select_review(cls, update, context):
        """ Меню проверки перед созданием инца """

        username = update.effective_user.username
        text = update.message.text
        update.message.delete()

        cls._temp_state[username]['description'] = text
        text = f"Назначить группе: <code>2 линия поддержки ОТТ</code>\n" \
               f"Инициатор: <code>{cls._temp_state[username]['initiator']}</code>\n" \
               f"Описание: \n<code>{cls._temp_state[username]['description']}</code>"
        keyboard = [
            [InlineKeyboardButton("✅ Создать инцидент", callback_data='complete')],
            [InlineKeyboardButton("🔄 Заполнить заново", callback_data='recreate')],
            [InlineKeyboardButton("❌ Отмена", callback_data='close')]
        ]
        context.bot.edit_message_text(chat_id=update.effective_chat.id,
                                      message_id=cls._temp_state[username].pop('mesId'),
                                      text=text,
                                      parse_mode='HTML',
                                      reply_markup=InlineKeyboardMarkup(keyboard))
        return cls.st_review

    @classmethod
    def _inc_create(cls, update, context, thread_start=False):
        """функция запускает саму себя в отдельном потоке"""
        username = update.effective_user.username

        if thread_start is False:
            context.bot.answer_callback_query(update.callback_query.id, text='')
            threading.Thread(target=cls._inc_create, args=(update, context, True), daemon=True).start()
            return ConversationHandler.END
        else:
            task = cls._temp_state.pop(username)

            # Меняем сообщение до ответа от СКУФ
            keyboard = [[InlineKeyboardButton("Создаю инцидент...", callback_data='None')]]
            update.callback_query.message.edit_text(text=update.callback_query.message.text, parse_mode='HTML',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

            # Добавляем задание на создание инца в очередь сокета
            response = services.skuf.create_task(task)
            result_create = response['result']

            if response['status_code'] == 200:
                text = f"INC: <code>{result_create['IncidentId']}</code>\n" \
                       f"Инициатор: <code>{task['initiator']}</code>\n" \
                       f"Описание: \n<code>{task['description']}</code>"

                # Создал ОТТ
                if username in database.Settings.get_access_4():
                    chat_id = database.Settings.get_setting('chat_id_inc')
                    keyboard = [[InlineKeyboardButton("Принять в работу 🔥",
                                                      callback_data=f"inc_accept:{result_create['IncidentId']}")]]
                    context.bot.send_message(chat_id=chat_id, text=text + '\n\n#new_inc', parse_mode='HTML',
                                             reply_markup=InlineKeyboardMarkup(keyboard))
                    if update.callback_query.message.chat.type == 'private':
                        update.callback_query.message.edit_text(text=f"{result_create['IncidentId']} "
                                                                     f"создан и отправлен в чат.",
                                                                reply_markup=InlineKeyboardMarkup(keyboard))
                    else:
                        update.callback_query.message.delete()

                else:
                    full_name_list = database.Users.get_fullname_by_username_tg(username).split()
                    full_name_skuf = ' '.join([full_name_list[0], full_name_list[1], full_name_list[2][:1]])
                    username_skuf = database.Users.get_username_skuf(username)
                    task_accept = {'type': 'inc_accept', 'inc': result_create['IncidentId'],
                                   'full_name_skuf': full_name_skuf, 'username_skuf': username_skuf}

                    # Назначаем инцидент на исполнителя
                    response = services.skuf.create_task(task_accept)
                    result_accept = response['result']
                    if response['status_code'] == 200:
                        keyboard = [[InlineKeyboardButton("Перевести в решён 📝",
                                                          callback_data=f"inc_resolved_menu:{result_create['IncidentId']}")]]
                        update.callback_query.message.edit_text(text=text, parse_mode='HTML',
                                                                reply_markup=InlineKeyboardMarkup(keyboard))
                    else:
                        text = f"Не удалось назначить {result_create['IncidentId']} на исполнителя. " \
                               f"Разработчику направлено уведомление о сбое: {result_accept}"
                        update.callback_query.message.edit_text(text=text, parse_mode='HTML')
                        logger.error(text)
            else:
                text = f'Произошла ошибка при создании инцидента, ' \
                       f'разработчику направлено уведомление о сбое: {str(result_create)}'
                update.callback_query.message.edit_text(text=text, parse_mode='HTML')
                logger.error(text)

    @classmethod
    def get_handler(cls):
        inc_create_handler = ConversationHandler(
            entry_points=[CommandHandler('create_inc', cls._select_initiator),
                          CallbackQueryHandler(cls._select_initiator, pattern='inc_create')],
            states={cls.st_initiator: [CallbackQueryHandler(cls._msg_description,
                                                            pattern=lambda i: i in cls.initiators)],
                    cls.st_description: [MessageHandler(Filters.text, cls._select_review)],
                    cls.st_review: [CallbackQueryHandler(cls._inc_create, pattern='complete'),
                                    CallbackQueryHandler(cls._select_initiator, pattern='recreate')]},
            fallbacks=[CallbackQueryHandler(cls._cancel)],
            conversation_timeout=180)
        return inc_create_handler

    @classmethod
    def _cancel(cls, update, context):
        username = update.effective_user.username
        query = update.callback_query.data
        cls._temp_state.pop(username, None)

        if query == 'close':
            update.callback_query.message.delete()
        elif query == 'back_general_menu':
            general_callback_handler(update, context)

        return ConversationHandler.END


class SkufIncAutoFAQ:
    """
    1. Проверяет очередь в SKUF на наличие INC созданных в AutoFAQ
    2. Отправляет информацию по INC в чат с возможностью принятия в работу
    3. Добавляет/удаляет номер INC в БД (во избежание создания дублей в чате)
    4. При принятии в работу/закрытии INC отправляет запрос в SKUF
    """

    res_desc = {1: 'Консультация по ЛК', 2: 'Консультация по настройке оборудования', 3: 'Настройка ЛК',
                4: 'Настройка оборудования', 5: 'Не ответил по КТ', 6: 'Проблема передана в ТБ',
                7: 'Проблема передана в КБ'}

    @classmethod
    def _inc_request(cls, updater, today, yesterday, thread_start=False):
        """Запрос INC из SKUF созданных в AutoFAQ"""

        task = {'type': 'inc_request', 'start_time': f'{yesterday}T00:00:01.001Z', 'end_time': f'{today}T23:59:59.001Z'}
        inc_request = services.skuf.create_task(task)
        result = inc_request['result']

        if inc_request['status_code'] == 200:
            if len(result) > 2:
                result = json.loads(result)

                for inc in result:
                    if thread_start is False:
                        threading.Thread(target=cls._inc_request, args=(updater, today, yesterday, True), daemon=True).start()
                    else:
                        incidents = database.SkufIncidents.get_inc()
                        if incidents:
                            if inc['Id'] in incidents:
                                continue 

                        task = {'type': 'inc_get_comment', 'inc': inc['Id']}
                        inc_get_comment = services.skuf.create_task(task)
                        comment_result = inc_get_comment['result']

                        if inc_get_comment['status_code'] == 200:
                            comment_result = json.loads(comment_result) 

                            database.SkufIncidents.add_inc(inc['Id']) 

                            text = f"INC: <code>{inc['Id']}</code>\n" \
                                    f"Инициатор: <code>{inc['Title']}</code>\n" \
                                    f"Описание: \n<code>{comment_result['Comments'][0]['Text']}</code>"

                            # Отправляем уведомление в чат
                            chat_id = database.Settings.get_setting('chat_id_inc')
                            keyboard = [[InlineKeyboardButton("Принять в работу 🔥",
                                                                callback_data=f"inc_accept:{inc['Id']}")]]
                            updater.bot.send_message(chat_id=chat_id, text=text + '\n\n#new_inc', parse_mode='HTML',
                                                            reply_markup=InlineKeyboardMarkup(keyboard))
                        else:
                            text = f'Произошла ошибка при запросе комментария INC AutoFAQ из СКУФ: {str(result)}'
                            logger.error(text)
        else:
            text = f'Произошла ошибка при запросе очереди INC AutoFAQ из СКУФ: {str(result)}'
            logger.error(text)

    @classmethod
    @database.auth_user(access='user')
    def _inc_accept(cls, update, context, thread_start=False):
        username = update.effective_user.username
        username_skuf = database.Users.get_username_skuf(username)

        if thread_start is False:
            params = dict(callback_query_id=update.callback_query.id, show_alert=False, text='')

            # Проверка chat_id пользователя
            if not database.Users.check_chat_id(username):
                params.update({'show_alert': True, 'text': f"Неизвестен ваш chat_id. Напишите боту в личку"})

            # Проверка наличия логина скуф в БД
            elif username_skuf == None:
                params.update({'show_alert': True, 'text': "Неизвестен ваш логин СКУФ. Обратитесь к @tndrcloud"})

            context.bot.answer_callback_query(**params)
            if not params['show_alert']:
                threading.Thread(target=cls._inc_accept, args=(update, context, True), daemon=True).start()
        else:
            analyst.skuf.accept_inc(username)
            query = update.callback_query.data.split(':')

            inc = query[1]
            full_name_list = database.Users.get_fullname_by_username_tg(username).split()
            full_name_skuf = ' '.join([full_name_list[0], full_name_list[1], full_name_list[2][:1]])

            task_accept = {'type': 'inc_accept', 'inc': inc, 'full_name_skuf': full_name_skuf,
                           'username_skuf': username_skuf}

            mes_orig = update.callback_query.message.text
            mes_accept = mes_orig.rstrip('#new_inc')
            keyboard = [[InlineKeyboardButton("Назначаю inc на исполнителя...", callback_data='None')]]
            update.callback_query.message.edit_text(text=mes_orig, parse_mode='HTML',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

            response = services.skuf.create_task(task_accept)

            if response['status_code'] == 200: 
                text = f'✅ {inc} принят в работу пользователем {full_name_skuf}.\n\n'
                text += mes_accept
                update.callback_query.message.edit_text(text=text, parse_mode='HTML')
                keyboard = [[InlineKeyboardButton("Перевести в решён 📝", callback_data=f'inc_resolved_menu:{inc}')]]
                chat_id = database.Users.get_user_chat_id(username)

                if chat_id:
                    context.bot.send_message(chat_id=chat_id, text=mes_accept, parse_mode='HTML',
                                             reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                keyboard = [[InlineKeyboardButton("Принять в работу 🔥", callback_data=update.callback_query.data)]]
                update.callback_query.message.edit_text(text=mes_orig, reply_markup=InlineKeyboardMarkup(keyboard))
                text = f'⛔ @{username} произошла ошибка принятия инцидента в работу. Err: {response["result"]}'
                context.bot.send_message(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
                logger.error(text)

    @classmethod
    def _inc_resolved(cls, update, context, thread_start=False):
        if thread_start is False:
            context.bot.answer_callback_query(update.callback_query.id, text='')
            threading.Thread(target=cls._inc_resolved, args=(update, context, True), daemon=True).start()
        else:
            username = update.effective_user.username
            analyst.skuf.resolved_inc(username)
            query = update.callback_query.data.split(':')
            inc = query[1]
            resolution = cls.res_desc[int(query[2])]
            task_resolved = {'type': 'inc_resolved', 'inc': inc, 'resolution': resolution}

            mes = update.callback_query.message.text
            keyboard = [[InlineKeyboardButton("Перевожу инцидент в решён..", callback_data='None')]]
            update.callback_query.message.edit_text(text=mes, parse_mode='HTML',
                                                    reply_markup=InlineKeyboardMarkup(keyboard))

            response = services.skuf.create_task(task_resolved)
            result = response['result']
            mes_inc_text = update.callback_query.message.text
            if response['status_code'] == 200:
                database.SkufIncidents.delete_inc(inc)
                text = f"✅ Инцидент решён: {resolution}\n\n{mes_inc_text}"
            else:
                text = f'⛔ Инцидент решить не удалось. Просьба решить в СКУФ ⛔\n\n{mes_inc_text}'
                logger.error(text + str(result))
            update.callback_query.message.edit_text(text=text, parse_mode='HTML')

    @classmethod
    def _inc_resolved_menu(cls, update, context):
        query = update.callback_query.data.split(':')
        inc = query[1]
        mes_text = update.callback_query.message.text
        keyboard = [[InlineKeyboardButton(v, callback_data=f"inc_resolved:{inc}:{k}")] for k, v in cls.res_desc.items()]
        update.callback_query.message.edit_text(text=mes_text, reply_markup=InlineKeyboardMarkup(keyboard))

    @classmethod
    def callback_handler(cls, update, context):
        query = update.callback_query.data

        if 'inc_accept:' in query:
            cls._inc_accept(update, context)
        elif 'inc_resolved_menu:' in query:
            cls._inc_resolved_menu(update, context)
        elif 'inc_resolved:' in query:
            cls._inc_resolved(update, context)
        else:
            return False
        return True


class SettingsMenu:
    _dialog_edit = 1
    _temp_state = {}

    @classmethod
    @database.auth_user(access='root')
    def _settings_menu(cls, update, context):
        username = update.effective_user.username
        cls._temp_state.pop(username, None)
        keyboard, row = [], []
        if WatchDog.run:
            wd_state = "Выключить watchdog"
        else:
            wd_state = "Включить watchdog"
        keyboard.append([InlineKeyboardButton(wd_state, callback_data='watchdog_control')])
        keyboard.append([InlineKeyboardButton("Скорректировать вызовы в БД", callback_data='fix_calls')])
        keyboard.append([InlineKeyboardButton("Получить лог файл", callback_data='get_log_file')])

        for type_, name in database.Settings.get_types_and_names():
            if len(type_) > 15:
                keyboard.append([InlineKeyboardButton(name, callback_data=f'setting_set:{type_}')])
            elif len(row) < 2:
                row.append(InlineKeyboardButton(name, callback_data=f'setting_set:{type_}'))
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
        if row:
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_general_menu')])
        update.callback_query.message.edit_text(text='Выберите параметр', reply_markup=InlineKeyboardMarkup(keyboard))
        return cls._dialog_edit

    @classmethod
    def watchdog_control(cls, update, context):
        WatchDog.run = False if WatchDog.run else True
        cls._settings_menu(update, context)

    @staticmethod
    def _fix_calls(update, context):
        database.Calls.fix_calls()
        context.bot.answer_callback_query(update.callback_query.id, text="Вызовы скорректированы!")

    @classmethod
    def _edit_setting(cls, update, context):
        username = update.effective_user.username
        keyboard = [[InlineKeyboardButton("🔙 Вернуться назад", callback_data='back_setting')]]

        if update.callback_query is not None:
            query = update.callback_query.data
            setting = query.split(':')[1]
            cls._temp_state[username] = {'type': setting, 'mes_id': update.callback_query.message.message_id}
            curr_value = database.Settings.get_setting(setting)
            message = f'Текущее значение {setting}: {curr_value}\nНапишите новое или вернитесь в предыдущее меню.'

        elif update.message is not None:
            new_value = update.message.text
            setting = cls._temp_state[username]['type']
            update.message.delete()
            database.Settings.set_setting(setting, new_value)
            message = f'Значение {setting} изменилось на {new_value}\nНапишите новое или вернитесь в предыдущее меню.'

        context.bot.edit_message_text(chat_id=update.effective_chat.id,
                                      message_id=cls._temp_state[username]['mes_id'],
                                      text=message, parse_mode='HTML',
                                      reply_markup=InlineKeyboardMarkup(keyboard))

    @staticmethod
    def _get_log_file(update, context):
        with open(logger.botlog_filename, 'rb') as file:
            file = file.read()
        context.bot.send_document(chat_id=update.effective_chat.id, document=file, filename=logger.botlog_filename)
        context.bot.answer_callback_query(update.callback_query.id, text="")

    @classmethod
    def _callback_handler(cls, update, context):
        username = update.effective_user.username
        cls._temp_state.pop(username, None)
        query = update.callback_query.data

        if query == 'settings_menu':
            return cls._settings_menu(update, context)
        elif query[:12] == 'setting_set:':
            cls._edit_setting(update, context)
        elif query == 'back_general_menu':
            menu(update, context, new_mes=False)
            return ConversationHandler.END
        elif query == 'back_setting':
            cls._settings_menu(update, context)
        elif query == 'get_log_file':
            cls._get_log_file(update, context)
            return cls._dialog_edit
        elif query == 'watchdog_control':
            cls.watchdog_control(update, context)
        elif query == 'fix_calls':
            cls._fix_calls(update, context)
        else:
            general_callback_handler(update, context)
            return ConversationHandler.END

    @classmethod
    def get_handler(cls):
        handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(cls._callback_handler, pattern='settings_menu')],
            states={cls._dialog_edit: [MessageHandler(Filters.text, cls._edit_setting)]},
            fallbacks=[CallbackQueryHandler(cls._callback_handler)],
            conversation_timeout=120)
        return handler
