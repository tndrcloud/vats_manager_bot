import os.path
import googleapiclient
import database
import threading
import time
import pandas
import log
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import httplib2
import calendar
import datetime
import requests
import os
from log import logger


class Schedule:
    """График поддерживает кеширование. Обновление данных происходит при запросе объекта не чаще чем
    раз в 15 минут."""

    def __init__(self):
        self._DUTY_TIME = '10.00 19.00'
        self._WEEKENDS = ['в', '!в', 'от', '!от', 'од', '!од', 'пк', 'б', '!б']
        self._schedule = {}
        self._lock = threading.Lock()
        self._request_time = None
        log.logger.info('create schedule')

    def _get_schedule(self, date):
        with self._lock:
            month = date.month
            curr_time = time.time()
            if not self._request_time or self._request_time + 900 < curr_time:
                self._request_time = curr_time
                self._schedule[month] = self._create_df(date)
        return self._schedule[month]

    def _create_df(self, date):
        month = date.strftime('%B')
        raw_sched = self._get_schedule_google(month)

        if not raw_sched:
            return

        users_data = database.Users.get_users_info()
        full_names = [data[3] for data in users_data]
        cal = calendar.Calendar()
        num_days = [str(day) for day in cal.itermonthdays(date.year, date.month) if day != 0]
        df = pandas.DataFrame(columns=['full_name', *num_days])
        df.month = month
        step = 1

        for row in raw_sched:
            # Пропускаем пустые строки
            if len(row) < 28:
                continue

            # ищем строку с числами месяца для определения индексов
            if step == 1:
                search_row_day = [day for day in row if day in num_days]
                if search_row_day == num_days:
                    i_start_day = row.index(num_days[0])
                    i_end_day = row.index(num_days[-1])
                    step = 2
                    continue

            # ищем строку с фио для определения индекса
            if step == 2:
                for cell in row:
                    if cell in full_names:
                        i_full_name = row.index(cell)
                        step = 3
                        break

            # собираем DataFrame
            if step == 3:
                if row[i_full_name] in full_names:
                    df.loc[len(df)] = [row[i_full_name], *row[i_start_day:i_end_day + 1]]
        return df

    @staticmethod
    def _get_schedule_google(month_en):
        translator_month_ru = {'January': 'Январь', 'February': 'Февраль', 'March': 'Март', 'April': 'Апрель',
                               'May': 'Май',
                               'June': 'Июнь', 'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь',
                               'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'}

        month_ru = translator_month_ru.get(month_en)

        if month_ru == None:
            return None

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']  # Области применения (
        # описание тут https://developers.google.com/identity/protocols/oauth2/scopes)

        # авторизация
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # файл из сервисного акк(подставить свой!)
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        SAMPLE_SPREADSHEET_ID = False  # ID таблицы
        service = build('sheets', 'v4', credentials=credentials).spreadsheets()

        info_table = service.get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
        list_names = [sheet['properties']['title'] for sheet in info_table['sheets']]
        if month_ru in list_names:
            # range - диапазон ячеек для вывода (если весь лист, то только его название)
            res = service.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=month_ru).execute()
            return res.get('values')
        else:
            raise KeyError(f'В таблице не найден лист {month_ru}')

    def _conv_time(self, time_: str):
        date = datetime.datetime.now()
        if time_.lower() in self._WEEKENDS:
            return False
        else:
            time_list = time_.split(" ")
            schedule_start = date.replace(hour=int(time_list[0].split('.')[0]),
                                          minute=int(time_list[0].split('.')[1]),
                                          second=0, microsecond=0)
            schedule_end = date.replace(hour=int(time_list[1].split('.')[0]),
                                        minute=int(time_list[1].split('.')[1]),
                                        second=0, microsecond=0)
            return [schedule_start, schedule_end]

    def get_work_time_user(self, date: datetime, fullname: str):
        schedule_ = self._get_schedule(date)
        work_time = schedule_[schedule_['full_name'] == fullname][str(date.day)]

        if not work_time.empty:
            return self._conv_time(work_time.to_string(index=False))

    def check_work_time_user(self, date: datetime, fullname: str):
        work_time = self.get_work_time_user(date, fullname)
        if work_time:
            if work_time[0] < date < work_time[1]:
                return True
            else:
                return False
        return work_time

    def get_schedule_today(self):
        date = datetime.datetime.now()
        schedule_ = self._get_schedule(date)
        users = schedule_[['full_name', str(date.day)]].to_dict()
        return {users['full_name'][i]: self._conv_time(users[str(date.day)][i]) for i in users['full_name']}

    def get_duty_today(self):
        """ Возвращает список с ФИО"""
        date = datetime.date.today()
        schedule_ = self._get_schedule(date)
        return schedule_[schedule_[str(date.day)] == self._DUTY_TIME]['full_name'].to_list()

    def get_duty_tomorrow(self):
        """ Возвращает список с ФИО"""
        date = datetime.date.today() + datetime.timedelta(days=1)
        schedule_ = self._get_schedule(date)
        return schedule_[schedule_[str(date.day)] == self._DUTY_TIME]['full_name'].to_list()

    @staticmethod
    def _create_schedule():
        SCOPES = ['https://www.googleapis.com/auth/documents.readonly',  # применение Google Docs
                  'https://www.googleapis.com/auth/spreadsheets',  # чтение и запись в Google Sheets
                  'https://www.googleapis.com/auth/drive']  # применение Google Drive

        SAMPLE_SPREADSHEET_ID = False  # ID таблицы

        TRANSLATOR_MONTH_EN = {'January': 'Январь', 'February': 'Февраль', 'March': 'Март', 'April': 'Апрель',
                               'May': 'Май', 'June': 'Июнь', 'July': 'Июль', 'August': 'Август', 'September':
                                   'Сентябрь', 'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'}

        SHEET_ID_SAMPLE = "2084223449"  # ID листа с шаблоном

        URL = 'https://hh.ru/calendar'

        HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                             'image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

        def create_new_sheet():
            creds_json = os.path.dirname(__file__) + "/credentials.json"
            creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, SCOPES).authorize(
                httplib2.Http())

            requests = build('sheets', 'v4', http=creds_service)
            body = {"requests": {"addSheet": {"properties": {"title": translate_month()}}}}
            resp = requests.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body=body).execute()

            return resp['replies'][0]['addSheet']['properties']['sheetId']  # создаём новый лист с названием месяца

        def read_employees():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = 'Шаблон!A5:C999'  # диапазон ячеек
            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()
            result = service.get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                 range=SAMPLE_RANGE_NAME).execute()

            data_from_sheet = result.get('values', [])  # извлекаем из шаблона список сотрудников

            return data_from_sheet

        def read_shedule():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = 'Шаблон!H5:N999'  # диапазон ячеек
            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()
            result = service.get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                 range=SAMPLE_RANGE_NAME).execute()

            shedule_from_sheet = result.get('values', [])  # извлекаем из шаблона график на неделю для сотрудников

            return shedule_from_sheet

        def write_sheet_title():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = f'{translate_month()}'  # диапазон ячеек
            data = {'values': [[], first_row, month_row, weeks_row]}  # заполняет "шапку" листа
            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()

            response = service.update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME,
                                      valueInputOption='USER_ENTERED',
                                      body=data).execute()

            return response

        def write_sheet_employees():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = f'{translate_month()}!A5'  # диапазон ячеек
            data = {'values': read_employees()}  # заполняет список сотрудников
            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()

            response = service.update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME,
                                      valueInputOption='USER_ENTERED',
                                      body=data).execute()

            return response

        def write_sheet_shedule():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = f'{translate_month()}!D5'

            # заполняет ячейки с графиком и временем работы сотрудников и формулами (кол-во сотрудников и т.д.)
            data = {'values': generator_shedule() +
                              [[], [], on_work_formula, missing_formula, [], duty_formula, check_friday]}

            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()

            response = service.update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME,
                                      valueInputOption='USER_ENTERED',
                                      body=data).execute()

            return response

        def write_appearance_day():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = f'{translate_month()}!{position_appearance_formula()}'  # диапазон ячеек
            data = {'values': [appearance_day_formula()]}  # заполняет ячейки формулами (дни явок, плановый фонд)
            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()

            response = service.update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME,
                                      valueInputOption='USER_ENTERED',
                                      body=data).execute()

            return response

        def write_number_employees():
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')  # JSON файл из сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

            SAMPLE_RANGE_NAME = f'{translate_month()}!A6'  # диапазон ячеек
            data = {'values': number_count_formula()}  # заполняет ячейки формулами с номером сотрудника в списке
            service = build('sheets', 'v4', credentials=credentials).spreadsheets().values()

            response = service.update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range=SAMPLE_RANGE_NAME,
                                      valueInputOption='USER_ENTERED',
                                      body=data).execute()

            return response

        def translate_month():
            for key in TRANSLATOR_MONTH_EN:  # возвращает перевод названия месяца
                if month_en == key:
                    return TRANSLATOR_MONTH_EN[key]

        def autofill_sheet(startIndex, endIndex, length):
            creds_json = os.path.dirname(__file__) + "/credentials.json"
            creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, SCOPES).authorize(
                httplib2.Http())
            request = build('sheets', 'v4', http=creds_service)

            body = {
                "requests": [  # автозаполнение ячеек для месяца с 28 днями
                    {
                        "autoFill": {  # автозаполнение ячеек (дни явок, плановый фонд, отсутствия)
                            "sourceAndDestination": {
                                "dimension": "ROWS",
                                "fillLength": count - 1,
                                "source": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 4,
                                    "endRowIndex": 5,
                                    "startColumnIndex": startIndex,
                                    "endColumnIndex": endIndex
                                }
                            },
                            "useAlternateSeries": False
                        }
                    },

                    {
                        "autoFill": {  # автозаполнение ячеек (кол-во сотрудников)
                            "sourceAndDestination": {
                                "dimension": "COLUMNS",
                                "fillLength": length,
                                "source": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": count + 6,
                                    "endRowIndex": count + 7,
                                    "startColumnIndex": 3,
                                    "endColumnIndex": 4
                                }
                            },
                            "useAlternateSeries": False
                        }
                    },

                    {
                        "autoFill": {  # автозаполнение ячеек (кол-во отсутствующих сотрудников)
                            "sourceAndDestination": {
                                "dimension": "COLUMNS",
                                "fillLength": length,
                                "source": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": count + 7,
                                    "endRowIndex": count + 8,
                                    "startColumnIndex": 3,
                                    "endColumnIndex": 4
                                }
                            },
                            "useAlternateSeries": False
                        }
                    },

                    {
                        "autoFill": {  # автозаполнение ячеек (наличие дежурства)
                            "sourceAndDestination": {
                                "dimension": "COLUMNS",
                                "fillLength": length,
                                "source": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": count + 9,
                                    "endRowIndex": count + 10,
                                    "startColumnIndex": 3,
                                    "endColumnIndex": 4
                                }
                            },
                            "useAlternateSeries": False
                        }
                    },

                    {
                        "autoFill": {  # автозаполнение ячеек (проверка пятниц)
                            "sourceAndDestination": {
                                "dimension": "COLUMNS",
                                "fillLength": length,
                                "source": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": count + 10,
                                    "endRowIndex": count + 11,
                                    "startColumnIndex": 3,
                                    "endColumnIndex": 4
                                }
                            },
                            "useAlternateSeries": False
                        }
                    }
                ]
            }

            resp = request.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body=body).execute()

            return resp

        def format_sheet(prm_1, prm_2, prm_3, prm_4, prm_5, prm_6, prm_7, prm_8, prm_9):
            creds_json = os.path.dirname(__file__) + "/credentials.json"
            creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, SCOPES).authorize(
                httplib2.Http())
            requests = build('sheets', 'v4', http=creds_service)

            body = {  # форматирование листа для месяца с 31 днём
                "requests": [
                    {
                        "copyPaste": {  # копирует форматирование из листа с шаблоном
                            "source": {
                                "sheetId": SHEET_ID_SAMPLE,
                                "startRowIndex": 0,
                                "endRowIndex": 1,
                            },

                            "destination": {
                                "sheetId": sheet_id,
                            },

                            "pasteType": "PASTE_FORMAT",
                            "pasteOrientation": "NORMAL"
                        }
                    },

                    {
                        "copyPaste": {  # копирует форматирование из листа с шаблоном
                            "source": {
                                "sheetId": SHEET_ID_SAMPLE,
                                "startRowIndex": 0,
                                "endRowIndex": 999,
                                "startColumnIndex": 0,
                                "endColumnIndex": prm_1,  # 1
                            },

                            "destination": {
                                "sheetId": sheet_id,
                                "startRowIndex": 0,
                                "startColumnIndex": 0
                            },

                            "pasteType": "PASTE_FORMAT",
                            "pasteOrientation": "NORMAL"
                        }
                    },

                    {
                        "copyPaste": {  # копирует форматирование из листа с шаблоном
                            "source": {
                                "sheetId": SHEET_ID_SAMPLE,
                                "startRowIndex": 0,
                                "endRowIndex": 39,
                                "startColumnIndex": prm_2,  # 2
                                "endColumnIndex": 37
                            },

                            "destination": {
                                "sheetId": sheet_id,
                                "startRowIndex": 0,
                                "endRowIndex": 39,
                                "startColumnIndex": prm_3,  # 3
                                "endColumnIndex": prm_4  # 4
                            },

                            "pasteType": "PASTE_FORMAT",
                            "pasteOrientation": "NORMAL"
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт ширину В столбца (Фамилия И.О.)
                            "properties": {
                                "pixelSize": 200
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 1,
                                "endIndex": 2,
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт ширину С столбца (Профессия, должность)
                            "properties": {
                                "pixelSize": 130
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 2,
                                "endIndex": 3,
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт ширину столбцов с днями недели
                            "properties": {
                                "pixelSize": 40
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 3,
                                "endIndex": prm_5,  # 5
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт ширину А столбца (Номер сотрудника в списке)
                            "properties": {
                                "pixelSize": 70
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 1,
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт ширину AH и AI столбцов (Дни явок, плановый фонд)
                            "properties": {
                                "pixelSize": 120
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": prm_6,  # 6
                                "endIndex": prm_7,  # 7
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт ширину AJ столбца (Отсутствия)
                            "properties": {
                                "pixelSize": 70
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": prm_8,  # 8
                                "endIndex": prm_9,  # 9
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт высоту 1 строки
                            "properties": {
                                "pixelSize": 20
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": 0,
                                "endIndex": 1
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт высоту 2 строки
                            "properties": {
                                "pixelSize": 45
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": 1,
                                "endIndex": 2
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт высоту 3 и 4 строки
                            "properties": {
                                "pixelSize": 20
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": 2,
                                "endIndex": 4
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт высоту ячеек с временем работы
                            "properties": {
                                "pixelSize": 35
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": 5,
                                "endIndex": 999
                            }
                        }
                    },

                    {
                        "updateDimensionProperties": {  # задаёт высоту ячеек с формулами
                            "properties": {
                                "pixelSize": 20
                            },

                            "fields": "pixelSize",
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": count + 4,
                                "endIndex": 999
                            }
                        }
                    },

                    {
                        "updateSheetProperties": {  # закрепляет первые 2 столбца и первые 4 строки
                            "properties": {
                                "sheetId": sheet_id,
                                "gridProperties": {
                                    "frozenRowCount": 4,
                                    "frozenColumnCount": 2
                                }
                            },

                            "fields": "gridProperties(frozenRowCount, frozenColumnCount)",
                        }
                    },
                ]
            }

            resp = requests.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body=body).execute()

            return resp

        def number_count_formula():
            numbers = []

            for var in range(5, count + 4):
                numbers.append([f'=A{var}+1'])

            return numbers

        def work_type_one():  # 5/2 (сб/вск) с 9 до 18 (18 сотрудников)
            type_one = []  # присваиваем этот список 5,6,7,8,9,10,11,13,14,15,16,17,18,19,20,21,22,23 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_one.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_one.append(f'=$A${count + 22}')
                            continue
                        elif day_name.weekday() == 4:
                            type_one.append(f'=$A${count + 22}')
                        elif day_name.weekday() < 5:
                            type_one.append(f'=$A${count + 21}')
                        else:
                            type_one.append(f'=$A${count + 13}')

            return type_one

        def work_type_two():  # 5/2 (сб/вск) с 6 до 15 (6 сотрудников)
            type_two = []  # присваиваем этот список 24,31,32,34,35,36 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_two.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_two.append(f'=$A${count + 35}')
                            continue
                        elif day_name.weekday() == 4:
                            type_two.append(f'=$A${count + 35}')
                        elif day_name.weekday() < 5:
                            type_two.append(f'=$A${count + 32}')
                        else:
                            type_two.append(f'=$A${count + 13}')

            return type_two

        def work_type_three():  # 5/2 (сб/вск) с 4 до 13 (4 сотрудника)
            type_three = []  # присваиваем этот список 26,27,29,33 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_three.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_three.append(f'=$A${count + 37}')
                            continue
                        elif day_name.weekday() == 4:
                            type_three.append(f'=$A${count + 37}')
                        elif day_name.weekday() < 5:
                            type_three.append(f'=$A${count + 34}')
                        else:
                            type_three.append(f'=$A${count + 13}')

            return type_three

        def work_type_four():  # 5/2 (сб/вск) с 8 до 17 (3 сотрудника)
            type_four = []  # присваиваем этот список 1,2,3 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_four.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_four.append(f'=$A${count + 25}')
                            continue
                        elif day_name.weekday() == 4:
                            type_four.append(f'=$A${count + 25}')
                        elif day_name.weekday() < 5:
                            type_four.append(f'=$A${count + 24}')
                        else:
                            type_four.append(f'=$A${count + 13}')

            return type_four

        def work_type_five():  # 5/2 (сб/вск) с 5 до 14 (3 сотрудника)
            type_five = []  # присваиваем этот список 25,28,30 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_five.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_five.append(f'=$A${count + 36}')
                            continue
                        elif day_name.weekday() == 4:
                            type_five.append(f'=$A${count + 36}')
                        elif day_name.weekday() < 5:
                            type_five.append(f'=$A${count + 33}')
                        else:
                            type_five.append(f'=$A${count + 13}')

            return type_five

        def work_type_six():  # 5/2 (вск/пн) с 9.30 до 18.30 (1 сотрудник)
            type_six = []  # присваиваем этот список 4 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_six.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_six.append(f'=$A${count + 28}')
                            continue
                        elif day_name.weekday() == 0:
                            type_six.append(f'=$A${count + 13}')
                        elif day_name.weekday() < 4:
                            type_six.append(f'=$A${count + 27}')
                        elif day_name.weekday() == 4:
                            type_six.append(f'=$A${count + 28}')
                        elif day_name.weekday() == 5:
                            type_six.append(f'=$A${count + 21}')
                        else:
                            type_six.append(f'=$A${count + 13}')

            return type_six

        def work_type_seven():  # 5/2 (вт/ср) с 9 до 18 (1 сотрудник)
            type_seven = []  # присваиваем этот список 5 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_seven.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_seven.append(f'=$A${count + 22}')
                            continue
                        elif day_name.weekday() == 0:
                            type_seven.append(f'=$A${count + 21}')
                        elif day_name.weekday() == 4:
                            type_seven.append(f'=$A${count + 22}')
                        elif day_name.weekday() >= 3:
                            type_seven.append(f'=$A${count + 21}')
                        else:
                            type_seven.append(f'=$A${count + 13}')

            return type_seven

        def work_type_eight():  # 5/2 (пт/сб) с 9 до 18 (1 сотрудник)
            type_eight = []  # присваиваем этот список 12 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_eight.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_eight.append(f'=$A${count + 22}')
                            continue
                        elif day_name.weekday() <= 3:
                            type_eight.append(f'=$A${count + 21}')
                        elif day_name.weekday() == 6:
                            type_eight.append(f'=$A${count + 21}')
                        else:
                            type_eight.append(f'=$A${count + 13}')

            return type_eight

        def work_type_nine():  # 5/2 (сб/вск) с 4 до 13 (1 сотрудник)
            type_nine = []  # присваиваем этот список 34 сотруднику в графике

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        if f'{day_name}' in holidays_list:
                            type_nine.append(f'=$A${count + 13}')
                            continue
                        elif f'{day_name}' in pre_holidays_list:
                            type_nine.append(f'=$A${count + 37}')
                            continue
                        elif day_name.weekday() == 4:
                            type_nine.append(f'=$A${count + 37}')
                        elif day_name.weekday() == 2:
                            type_nine.append(f'=$A${count + 37}')
                        elif day_name.weekday() < 5:
                            type_nine.append(f'=$A${count + 34}')
                        else:
                            type_nine.append(f'=$A${count + 13}')

            return type_nine

        def appearance_day_formula():
            if calendar.isleap(next_year.year) == True:  # проверка високостного года
                if next_month == 2:
                    return variable3
                exit()

            if next_month in [1, 3, 5, 7, 8, 10, 12]:  # заполнение формулой в соотв. с месяцем в году
                return variable1
            elif next_month in [4, 6, 9, 11]:
                return variable2
            else:
                return variable4

        def position_appearance_formula():
            if calendar.isleap(next_year.year) == True:  # проверка високостного года
                if next_month == 2:
                    return 'AG5'
                exit()

            if next_month in [1, 3, 5, 7, 8, 10, 12]:  # возвращает ячейку для заполнения в соотв. с месяцем в году
                return 'AI5'
            elif next_month in [4, 6, 9, 11]:
                return 'AH5'
            else:
                return 'AF5'

        def counter():
            count = 0  # функция считает кол-во сотрудников из шаблона и возвращает число

            for index in employees:
                if index == []:
                    break
                if not len(index) == 0:
                    count += 1

            return count

        def generator_shedule():
            work_time = []

            for week in shedule:  # генерирует двухмерных массив с временем работы сотрудников
                if week == []:
                    break
                elif week == ['08.00 17.00', '08.00 17.00', '08.00 17.00', '08.00 17.00', '08.00 16.00', 'В', 'В']:
                    work_time.append(work_type_four())
                    continue
                elif week == ['В', '09.30 18.30', '09.30 18.30', '09.30 18.30', '09.30 17.30', '09.00 18.00', 'В']:
                    work_time.append(work_type_six())
                    continue
                elif week == ['09.00 18.00', 'В', 'В', '09.00 18.00', '09.00 17.00', '09.00 18.00', '09.00 18.00']:
                    work_time.append(work_type_seven())
                    continue
                elif week == ['09.00 18.00', '09.00 18.00', '09.00 18.00', '09.00 18.00', '09.00 17.00', 'В', 'В']:
                    work_time.append(work_type_one())
                    continue
                elif week == ['09.00 18.00', '09.00 18.00', '09.00 18.00', '09.00 18.00', 'В', 'В', '09.00 18.00']:
                    work_time.append(work_type_eight())
                    continue
                elif week == ['06.00 15.00', '06.00 15.00', '06.00 15.00', '06.00 15.00', '06.00 14.00', 'В', 'В']:
                    work_time.append(work_type_two())
                    continue
                elif week == ['05.00 14.00', '05.00 14.00', '05.00 14.00', '05.00 14.00', '05.00 13.00', 'В', 'В']:
                    work_time.append(work_type_five())
                    continue
                elif week == ['04.00 13.00', '04.00 13.00', '04.00 12.00', '04.00 13.00', '04.00 12.00', 'В', 'В']:
                    work_time.append(work_type_nine())
                    continue
                else:
                    work_time.append(work_type_three())
                    continue

            return work_time

        def year_calendar_list():
            cld = calendar.Calendar()  # функция формирует календарь на год с датами в формате date.datetime
            now = datetime.datetime.now()
            next_year = now + datetime.timedelta(days=31)

            if now.month == 12:
                next_month = next_year.month
            else:
                next_month = 1

            now_data_cld = now.year, next_month
            next_data_cld = now.year + 1, next_month

            if now.month == 12:
                month = cld.monthdatescalendar(*next_data_cld)
            else:
                month = cld.monthdatescalendar(*now_data_cld)

            calendar_list = []

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 1)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 1:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 2)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 2:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 3)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 3:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 4)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 4:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 5)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 5:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 6)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 6:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 7)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 7:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 8)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 8:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 9)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 9:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 10)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 10:
                        calendar_list.append(day_name)
                    else:
                        month = cld.monthdatescalendar(now.year, next_month + 11)
                        continue
            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month + 11:
                        calendar_list.append(day_name)
                    else:
                        continue

            return calendar_list

        def get_html(url, params=None):
            request = requests.get(url, headers=HEADERS, params=params)  # http запрос к ресурсу

            return request

        def get_content(html):
            soup = BeautifulSoup(html, 'html.parser')  # функция для парсинга ресурса
            work_day = soup.find_all('li', class_='calendar-list__numbers__item')

            request_list = []
            for w in work_day:
                request_list.append(w.get_text(strip=True))

            daysoff_list = []
            for l in request_list:
                if 0 < len(l) < 68:
                    daysoff_list.append(l)

            return daysoff_list

        def parse_hh():
            html = get_html(URL)  # функция возвращает результат запроса
            if html.status_code == 200:
                return get_content(html.text)
            else:
                logger.error('Ошибка при парсинге ресурса! Повторите позже.')

        def holiday_check():
            holiday_and_weekend_date = []  # функция возвращает список выходных, праздничных и предпраздничных дней
            pre_holiday_date = []
            only_holiday_date = []

            for week_name in month:
                for day_name in week_name:
                    if day_name.month == next_month:
                        for key, value in calendarIndex.items():
                            if 'Предпраздничный день' in value:
                                pre_holiday_date.append(str(key))
                            elif 'Новый год' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'Новогодние каникулы' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'Рождество Христово' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'День защитника Отечества' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'Международный женский день' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'Праздник Весны и Труда' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'День Победы' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'День России' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'День народного единства' in value:
                                holiday_and_weekend_date.append(str(key)), only_holiday_date.append(str(key))
                            elif 'Выходной' in value:
                                if key.weekday() < 5:
                                    holiday_and_weekend_date.append(str(key))

            return holiday_and_weekend_date, pre_holiday_date, only_holiday_date

        def get_marks(prm_1, prm_2, prm_3, prm_4, prm_5, prm_6, prm_7, startIndex, endIndex):
            creds_json = os.path.dirname(__file__) + "/credentials.json"
            creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, SCOPES).authorize(
                httplib2.Http())
            requests = build('sheets', 'v4', http=creds_service)

            body = {  # закрашивает ячейки выходных, праздничных и предпраздничных дней
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": prm_1,  # 1
                                "endRowIndex": prm_2,  # 2
                                "startColumnIndex": startIndex,
                                "endColumnIndex": endIndex
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {
                                        "red": prm_3,  # 3
                                        "green": prm_4,  # 4
                                        "blue": prm_5  # 5
                                    },
                                    "textFormat": {
                                        "foregroundColor": {
                                            "red": prm_6,  # 6
                                            "green": prm_7,  # 7
                                            "blue": 0
                                        },
                                        "fontFamily": "Calibri",
                                        "fontSize": 9,
                                    }
                                }
                            },

                            "fields": "userEnteredFormat(backgroundColor, textFormat)"
                        }
                    },
                ]
            }

            resp = requests.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body=body).execute()

            return resp

        cld = calendar.Calendar()
        now = datetime.datetime.now()  # текущая дата
        days_in_month = calendar.monthrange(now.year, now.month)[1]
        next_year = now + datetime.timedelta(days=days_in_month)

        if now.month == 12:  # если текущий месяц - декабрь, то следующий месяц это первый месяц следующего года
            next_month = next_year.month
        else:
            next_month = now.month + 1  # следующий месяц от текущей даты

        now_data_cld = now.year, next_month
        next_data_cld = now.year + 1, next_month

        WEEK_DAYS_NAME = ('пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс')

        if now.month == 12:
            month = cld.monthdatescalendar(*next_data_cld)
            days_on_month = calendar.monthrange(*next_data_cld)
        else:
            month = cld.monthdatescalendar(*now_data_cld)
            days_on_month = calendar.monthrange(*now_data_cld)

        first_row = ['', 'Фамилия, И.О.', 'Профессия, должность', 'Числа месяца']

        for i in range(days_on_month[1] - 1):  # считаем кол-во отступов в "шапке" листа
            first_row.append('')

        first_row.extend(['Дни явок по графику', 'Плановый фонд рабочего времени, час.', 'Отсутствия'])

        month_row = ['', 'Месяц', '']

        month_en = month[1][0].strftime('%B')
        weeks_row = ['', translate_month(), '']
        month_days = []

        for week_name in month:  # проставляем кол-во дней в месяце и день недели соответствующий числу
            for day_name in week_name:
                if day_name.month == next_month:
                    month_row.append(day_name.day)
                    weeks_row.append(WEEK_DAYS_NAME[day_name.weekday()])
                    month_days.append(day_name)

        sheet_id = create_new_sheet()  # результаты некоторых функций передаём в переменные
        employees = read_employees()
        shedule = read_shedule()
        count = counter()
        parse_content = parse_hh()
        calendar_list = year_calendar_list()
        calendarIndex = dict(zip(calendar_list, parse_content))
        holidays_list, pre_holidays_list, only_holidays_list = holiday_check()

        on_work_formula = [f'=СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 21})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 24})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 30})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 32})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 33})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 27})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 34})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 22})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 25})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 28})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 35})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 36})'
                           f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 37})']

        missing_formula = [f'=ЕСЛИ($A${count + 4}-D{count + 7}=$A${count + 4}'
                           f'-2;0;$A${count + 4}-D{count + 7})']

        duty_formula = [f'=СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 30})']

        check_friday = [f'=ЕСЛИ(СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 21})'
                        f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 24})'
                        f'+СЧЁТЕСЛИ(D5:D{count + 4};$A${count + 27});0;1)']

        variable1 = [f'=СЧЁТЕСЛИ(A5:AH5;$A${count + 21})+СЧЁТЕСЛИ(A5:AH5;$A${count + 22})'  # 31 день
                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 24})+СЧЁТЕСЛИ(A5:AH5;$A${count + 25})'
                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 27})+СЧЁТЕСЛИ(A5:AH5;$A${count + 28})'
                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 30})+СЧЁТЕСЛИ(A5:AH5;$A${count + 32})'
                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 33})+СЧЁТЕСЛИ(A5:AH5;$A${count + 34})'
                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 35})+СЧЁТЕСЛИ(A5:AH5;$A${count + 36})'
                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 37})', '=AI5*8', f'=СЧЁТЕСЛИ(A5:AH5;$A${count + 40})'
                                                                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 42})+СЧЁТЕСЛИ(A5:AH5;$A${count + 44})'
                                                                     f'+СЧЁТЕСЛИ(A5:AH5;$A${count + 45})']

        variable2 = [f'=СЧЁТЕСЛИ(A5:AG5;$A${count + 21})+СЧЁТЕСЛИ(A5:AG5;$A${count + 22})'  # 30 дней
                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 24})+СЧЁТЕСЛИ(A5:AG5;$A${count + 25})'
                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 27})+СЧЁТЕСЛИ(A5:AG5;$A${count + 28})'
                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 30})+СЧЁТЕСЛИ(A5:AG5;$A${count + 32})'
                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 33})+СЧЁТЕСЛИ(A5:AG5;$A${count + 34})'
                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 35})+СЧЁТЕСЛИ(A5:AG5;$A${count + 36})'
                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 37})', '=AH5*8', f'=СЧЁТЕСЛИ(A5:AG5;$A${count + 40})'
                                                                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 42})+СЧЁТЕСЛИ(A5:AG5;$A${count + 44})'
                                                                     f'+СЧЁТЕСЛИ(A5:AG5;$A${count + 45})']

        variable3 = [f'=СЧЁТЕСЛИ(A5:AF5;$A${count + 21})+СЧЁТЕСЛИ(A5:AF5;$A${count + 22})'  # 29 дней
                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 24})+СЧЁТЕСЛИ(A5:AF5;$A${count + 25})'
                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 27})+СЧЁТЕСЛИ(A5:AF5;$A${count + 28})'
                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 30})+СЧЁТЕСЛИ(A5:AF5;$A${count + 32})'
                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 33})+СЧЁТЕСЛИ(A5:AF5;$A${count + 34})'
                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 35})+СЧЁТЕСЛИ(A5:AF5;$A${count + 36})'
                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 37})', '=AG5*8', f'=СЧЁТЕСЛИ(A5:AF5;$A${count + 40})'
                                                                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 42})+СЧЁТЕСЛИ(A5:AF5;$A${count + 44})'
                                                                     f'+СЧЁТЕСЛИ(A5:AF5;$A${count + 45})']

        variable4 = [f'=СЧЁТЕСЛИ(A5:AE5;$A${count + 21})+СЧЁТЕСЛИ(A5:AE5;$A${count + 22})'  # 28 дней
                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 24})+СЧЁТЕСЛИ(A5:AE5;$A${count + 25})'
                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 27})+СЧЁТЕСЛИ(A5:AE5;$A${count + 28})'
                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 30})+СЧЁТЕСЛИ(A5:AE5;$A${count + 32})'
                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 33})+СЧЁТЕСЛИ(A5:AE5;$A${count + 34})'
                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 35})+СЧЁТЕСЛИ(A5:AE5;$A${count + 36})'
                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 37})', '=AF5*8', f'=СЧЁТЕСЛИ(A5:AE5;$A${count + 40})'
                                                                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 42})+СЧЁТЕСЛИ(A5:AE5;$A${count + 44})'
                                                                     f'+СЧЁТЕСЛИ(A5:AE5;$A${count + 45})']

        columnsIndex = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                        18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]

        listIndex = dict(zip(columnsIndex, weeks_row))  # собираем два списка в словарь
        listDaysMonth = dict(zip(columnsIndex, month_days))

        write_sheet_title()  # вызов функций для записи в лист
        write_sheet_employees()
        write_sheet_shedule()
        write_number_employees()
        write_appearance_day()

        if calendar.isleap(next_year.year) == True:  # проверка високосного года
            if next_month == 2:
                format_sheet(32, 34, 32, 35, 32, 32, 34, 34, 35), autofill_sheet(32, 35, 28)

            for key, value in listIndex.items():  # проверка ячеек (СБ)
                if value in ['сб', 'вс']:
                    startIndexWeekday = key
                    endIndexWeekday = startIndexWeekday + 1
                    get_marks(3, 4, 0.7, 0, 0, 1, 1, startIndexWeekday, endIndexWeekday)

            for key, value in listDaysMonth.items():  # проверка праздничных дней
                if str(value) in holidays_list:
                    startIndexHoliday = key + 3
                    endIndexHoliday = startIndexHoliday + 1
                    get_marks(2, count + 4, 0.87, 0.57, 0.57, 0, 0, startIndexHoliday, endIndexHoliday)

            for key, value in listDaysMonth.items():  # проверка предпраздничных дней
                if str(value) in pre_holidays_list:
                    startIndexPre = key + 3
                    endIndexPre = startIndexPre + 1
                    get_marks(2, count + 4, 0.95, 0.75, 0.75, 0, 0, startIndexPre, endIndexPre)
            return

        if next_month in [1, 3, 5, 7, 8, 10, 12]:  # автозаполнение и форматирование листа в соотв. с месяцем в году
            format_sheet(34, 33, 33, 36, 34, 34, 36, 36, 37), autofill_sheet(34, 37, 30)
        elif next_month in [4, 6, 9, 11]:
            format_sheet(33, 33, 32, 35, 33, 33, 35, 35, 36), autofill_sheet(33, 36, 29)
        else:
            format_sheet(31, 33, 30, 33, 31, 31, 33, 33, 34), autofill_sheet(31, 34, 27)

        for key, value in listIndex.items():  # проверка ячеек (СБ и ВС)
            if value in ['сб', 'вс']:
                startIndexWeekday = key
                endIndexWeekday = startIndexWeekday + 1
                get_marks(3, 4, 0.7, 0, 0, 1, 1, startIndexWeekday, endIndexWeekday)

        for key, value in listDaysMonth.items():  # проверка праздничных дней
            if str(value) in only_holidays_list:
                startIndexHoliday = key + 3
                endIndexHoliday = startIndexHoliday + 1
                get_marks(2, count + 4, 0.87, 0.57, 0.57, 0, 0, startIndexHoliday, endIndexHoliday)

        for key, value in listDaysMonth.items():  # проверка предпраздничных дней
            if str(value) in pre_holidays_list:
                startIndexPre = key + 3
                endIndexPre = startIndexPre + 1
                get_marks(2, count + 4, 0.95, 0.75, 0.75, 0, 0, startIndexPre, endIndexPre)

    @classmethod
    def create_next_month(cls):
        try:
            cls._create_schedule()
            return 1
        except googleapiclient.errors.HttpError:
            return 2
        except Exception as err:
            logger.error(f'Не удалось создать график: {err}')
            return 3


schedule = Schedule()

