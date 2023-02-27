import asyncio
import logging
import websockets
import hashlib
import json
import os
import subprocess
import re


peers = ["S001.14.rt.ru_reg/sip:S001.14.rt.ru", "S001.16.rt.ru_reg/sip:S001.16.rt.ru",
         "klyuch_reg/sip:SOCO3.14.rt.ru", "nop_reg/sip:SOCO2.14.rt.ru", "ud_reg/sip:SOCO3.14.rt.ru",
         "uo_reg/sip:SOCO3.14.rt.ru", "vats_reg/sip:SOCO2.14.rt.ru", "vkrt_reg/sip:SOCO3.14.rt.ru",
         "wifi_reg/sip:S001.14.rt.ru"]


users = ["autoinc", "autoinc", "asterisk_zaglushka_klyuch", "asterisk_zaglushka_nop",
         "asterisk_zaglushka_ud", "asterisk_zaglushka_uo", "asterisk_zaglushka_vats",
         "asterisk_zaglushka_vkrt", "asterisk_zaglushka_wifi"]


async def keepalive(queue_vmsp, task):
    result = {}

    try:
        for i in range(len(peers)):
            request = subprocess.Popen("sudo asterisk -rx 'pjsip show registrations' | grep {}".format(peers[i]), shell=True, stdout=subprocess.PIPE)
            response = str(request.communicate()[0])
            registered = bool(re.search(r'Registered', response))
            if registered == True:
                result["{}".format(peers[i])] = {"{}".format(users[i]): "Registered"}
            else:
                result["{}".format(peers[i])] = {"{}".format(users[i]): "Unregistered"}

        await queue_vmsp.put((task, 200, result))
    except Exception as err:
        await queue_vmsp.put((task, 400, err))
        logging.error(err)


async def register(queue_vmsp, task, peer):
    sip_reg = peer.split('/')[0]

    try:
        request = subprocess.Popen("sudo asterisk -rx 'pjsip send register {}'".format(sip_reg), shell=True, stdout=subprocess.PIPE)
        response = str(request.communicate()[0])
        await asyncio.sleep(15)

        await queue_vmsp.put((task, 200, response))
    except Exception as err:
        await queue_vmsp.put((task, 400, err))
        logging.error(err)


async def main():
    def sha256(data):
        hash_object = hashlib.sha256(bytes(False + data + False, encoding='utf-8'))
        hex_dig = hash_object.hexdigest()
        return hex_dig

    # Создаем очередь
    queue_vmsp = asyncio.Queue()

    # Отправляет результаты
    async def sender(ws):
        while ws.open:
            try:
                task_id, status_code, result = queue_vmsp.get_nowait()
                await ws.send(json.dumps({'task_id': task_id, 'status_code': status_code, 'result': result}))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)

    while True:
        try:
            async with websockets.connect(False) as ws:
                # Авторизация
                await ws.send(False)
                salt = await ws.recv()
                await ws.send(sha256(salt))
                await ws.send('vmsp')
                logging.info('ws client connecting')

                # Запуск отправителя результатов
                loop = asyncio.get_event_loop()
                loop.create_task(sender(ws))

                # Получаем задания
                async for message in ws:
                    task = json.loads(message)

                    # Отправляем асинхронно в СКУФ
                    if task['type'] == 'keep_alive_request':
                        loop.create_task(keepalive(queue_vmsp, task['task_id']))
                    elif task['type'] == 'send_retry_register':
                        loop.create_task(register(queue_vmsp, task['task_id'], task['peer']))

        except Exception as err:
            logging.error(err, exc_info=True)
            await asyncio.sleep(3)

log_file = os.path.dirname(__file__) + '/ws_client.log'
if not os.path.isfile(log_file):
    open(log_file, 'w').close()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                level=logging.WARNING, filename=log_file)

# Start
asyncio.run(main())
