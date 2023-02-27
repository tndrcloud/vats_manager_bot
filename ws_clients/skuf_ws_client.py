import asyncio
import logging
from unittest import result
import websockets
import hashlib
import json
import aiohttp
import os


TEST = False


def get_logger():
    logger = logging.getLogger('vats manager bot')
    format_ = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:file %(module)s line %(lineno)d:%(message)s')

    # File log
    f_handler = logging.FileHandler(os.path.dirname(__file__) + 'skuf.log')
    f_handler.setLevel(logging.INFO)
    f_handler.setFormatter(format_)
    logger.addHandler(f_handler)

    # Console log
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    c_format = logging.Formatter('%(name)s - %(levelname)s - file %(module)s line %(lineno)d - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    logger.setLevel(logging.DEBUG)
    return logger


logger = get_logger()


async def autoraized():
    url = "http://10.79.1.204:7080/api/v1/auth"
    payload = {"username": False,
               "password": False}
    
    headers = {'Content-Type': 'application/json'}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=json.dumps(payload), headers=headers) as response:
            if response.status == 200:
                token = f'Bearer {(await response.json())["token"]}'
            else:
                token = None
    return token


async def request_inc(queue_skuf, task_id, status, start_date, end_date):
    try:
        url = f"http://10.79.1.204:7080/api/v1/incidents/"

        params = {
            "Service": "Виртуальная АТС",
            "StartDate": f"{start_date}",
            "EndDate": f"{end_date}",
            "StatusType": f"{status}",
            "Requester": "Инсталлятор AutoFAQ"
        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{await autoraized()}'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    await queue_skuf.put((task_id, response.status, json.loads(await response.text())))
                else:
                    await queue_skuf.put((task_id, response.status, await response.text()))
    except Exception as err:
        await queue_skuf.put((task_id, 400, err))
        logger.error(err)


async def get_comment_inc(queue_skuf, task_id, inc):
    try:
        url = f"http://10.79.1.204:7080/api/v1/incidents/{inc}"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{await autoraized()}'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    await queue_skuf.put((task_id, response.status, json.loads(await response.text())))
                else:
                    await queue_skuf.put((task_id, response.status, await response.text()))
    except Exception as err:
        await queue_skuf.put((task_id, 400, err))
        logger.error(err)


async def actions_inc(queue_skuf, task_id, action, inc, resolution):
    try:
        url = f"http://10.79.1.204:7080/api/v1/incidents/{inc}"

        payload = {
            "status": {
                "type": f"{action}",
                "changeReason": "No Further Action Required",
                "resolution": f"{resolution}"
            }
        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{await autoraized()}'
        }

        async with aiohttp.ClientSession() as session:
            async with session.patch(url, data=json.dumps(payload), headers=headers) as response:
                if response.status == 200:
                    # response.json() именно от скуф возвращает str
                    await queue_skuf.put((task_id, response.status, json.loads(await response.text())))
                else:
                    await queue_skuf.put((task_id, response.status, await response.text()))
    except Exception as err:
        await queue_skuf.put((task_id, 400, err))
        logger.error(err)


async def inc_accept(queue_skuf, task_id, inc, username_skuf, full_name_skuf):
    try:
        url = f"http://10.79.1.204:7080/api/v1/incidents/{inc}"

        payload = {
            "status": {
                "type": "In Progress",
                "changeReason": "",
                "resolution": ""
            },
            "group": {
                "title": "2 линия поддержки ОТТ",
                "id": "SGP000000025115",
                "company": "Ростелеком",
                "organization": "ДЭФИР"
            },
            "assignee": {
                "name": f"{full_name_skuf}",
                "login": f"{username_skuf}"
            }
        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{await autoraized()}'
        }

        async with aiohttp.ClientSession() as session:
            async with session.patch(url, data=json.dumps(payload), headers=headers) as response:
                if response.status == 200:
                    # response.json() именно от скуф возвращает str
                    await queue_skuf.put((task_id, response.status, json.loads(await response.text())))
                else:
                    await queue_skuf.put((task_id, response.status, await response.text()))
    except Exception as err:
        await queue_skuf.put((task_id, 400, err))
        logger.error(err)


async def inc_create(queue_skuf, task_id, description, initiator):
    try:
        url = "http://10.79.1.204:7080/api/v1/incidents"
        payload = json.dumps({"zone": "Блок платформ РТК",
                              "service": "Виртуальная АТС",
                              "title": f"{description[:98]}",
                              "description": f"{description}",
                              "priority": "Medium",
                              "urgency": "2-Regular",
                              "type": "Incident",
                              "requester": {"fullName": f"{initiator}",
                                            "login": "autofaq.intergration"},
                              "group": {"title": "2 линия поддержки ОТТ",
                                        "id": "SGP000000025115",
                                        "company": "Ростелеком",
                                        "organization": "ДЭФИР"}})

        headers = {'Content-Type': 'application/json',
                   'Authorization': f'{await autoraized()}'}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, headers=headers) as response:
                if response.status == 200:
                    # response.json() именно от скуф возвращает str, много экранирования
                    await queue_skuf.put((task_id, response.status, json.loads(await response.json())))
                else:
                    await queue_skuf.put((task_id, response.status, await response.text()))
    except Exception as err:
        await queue_skuf.put((task_id, 400, err))
        logger.error(err)


async def main(server_ws_addr):
    def sha256(data):
        hash_object = hashlib.sha256(bytes(False + data + False, encoding='utf-8'))
        hex_dig = hash_object.hexdigest()
        return hex_dig

    # Создаем очередь
    queue_skuf = asyncio.Queue()

    # Отправляет результаты
    async def sender(ws):
        while ws.open:
            try:
                task_id, status_code, result = queue_skuf.get_nowait()
                await ws.send(json.dumps({'task_id': task_id, 'status_code': status_code, 'result': result}))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except TypeError as err:
                logger.error(err)
                await asyncio.sleep(0.1)

    while True:
        try:
            async with websockets.connect(server_ws_addr) as ws:
                # Авторизация
                await ws.send(False)
                salt = await ws.recv()
                await ws.send(sha256(salt))
                await ws.send('skuf')
                logging.info('ws client connecting')
                # Запуск отправителя результатов
                loop = asyncio.get_event_loop()
                loop.create_task(sender(ws))

                # Получаем задания
                async for message in ws:
                    task = json.loads(message)

                    # Отправляем асинхронно в СКУФ
                    if task['type'] == 'inc_create':
                        loop.create_task(inc_create(queue_skuf, task['task_id'], task['description'], task['initiator']))
                    elif task['type'] == 'inc_accept':
                        loop.create_task(inc_accept(queue_skuf, task['task_id'], task['inc'], task['username_skuf'], task['full_name_skuf']))
                    elif task['type'] == 'inc_resolved':
                        loop.create_task(actions_inc(queue_skuf, task['task_id'], 'Resolved', task['inc'], task['resolution']))
                    elif task['type'] == 'inc_request':
                        loop.create_task(request_inc(queue_skuf, task['task_id'], 'Assigned', task['start_time'], task['end_time']))
                    elif task['type'] == 'inc_get_comment':
                        loop.create_task(get_comment_inc(queue_skuf, task['task_id'], task['inc']))

        except Exception as err:
            logger.error(err, exc_info=True)
            await asyncio.sleep(3)


if TEST:
    server_ws_addr = False
else:
    server_ws_addr = False

# Start
asyncio.run(main(server_ws_addr))
