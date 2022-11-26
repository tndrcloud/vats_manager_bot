import logging
import database
import requests
import json
import os


class TelegramHandler(logging.Handler):
    def send_message(self, text):
        url = 'https://api.telegram.org/bot'
        token = database.Settings.get_token()
        metod = '/sendMessage'
        params = {'chat_id': False, 
                  'text': text}

        r = requests.post(url + token + metod, data=params)
        return json.loads(r.text)

    def emit(self, record):
        try:
            self.send_message(text=self.formatter.format(record))
        except Exception:
            pass


def get_logger():
    logger = logging.getLogger('vats manager bot')
    format_ = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:file %(module)s line %(lineno)d:%(message)s')

    # File log
    file_name = os.path.dirname(__file__) + "/bot.log"
    f_handler = logging.FileHandler(file_name)
    f_handler.setLevel(logging.INFO)
    f_handler.setFormatter(format_)
    logger.botlog_filename = file_name
    logger.addHandler(f_handler)

    # telegram log
    t_handler = TelegramHandler()
    t_handler.setLevel(logging.WARNING)
    t_handler.setFormatter(format_)
    logger.addHandler(t_handler)

    # Console log
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    c_format = logging.Formatter('%(name)s - %(levelname)s - file %(module)s line %(lineno)d - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    if database.Settings.get_working_mode() == 'prod':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.DEBUG)

    return logger


logger = get_logger()
