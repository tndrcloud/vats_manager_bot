import asyncio
import aiohttp
import fake_useragent
import websockets
import hashlib
import json
import logging

def get_logger():
    logger = logging.getLogger('vats manager bot')
    format_ = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:file %(module)s line %(lineno)d:%(message)s')

    # File log
    f_handler = logging.FileHandler('history_bot.log')
    f_handler.setLevel(logging.INFO)
    f_handler.setFormatter(format_)
    logger.addHandler(f_handler)

    # Console log
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - file %(module)s line %(lineno)d - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    logger.setLevel(logging.DEBUG)
    return logger


TEST = False
logger = get_logger()


class ConnectorNTTM:
    def __init__(self, loop):
        self._email = False
        self._passwd = False
        
        if TEST:
            self._url = False
        else:
            self._url = False

        self._user_agent = fake_useragent.UserAgent().random
        self.tasks = asyncio.Queue()
        self.results = asyncio.Queue()

        self._work = 0
        self._max_worker = 5
        self._session = []

        self._session_update = asyncio.Event()
        self._session_error = asyncio.Event()
        self._session_error.set()

        self._loop = loop
        loop.create_task(self._core())

    async def _create_session(self):
        # for _ in range(5)
        while True:
            session = aiohttp.ClientSession()
            payload = json.dumps(dict(username=self._email, password=self._passwd, force=True))
            headers = {'Accept': 'application/json, text/plain, */*', 'User-Agent': self._user_agent,
                       'Content-Type': 'application/json'}
            METOD = "/nttm-task-handler/api/authenticate"
            response = await session.post(self._url + METOD, data=payload, headers=headers)

            if response.status == 200:
                headers = {
                    'User-Agent': self._user_agent,
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + (await response.json())['id_token']
                }

                self._session = [session, headers]
                self._session_update.set()
                self._session_error.set()
                break
            else:
                logger.error(await response.text())
                await asyncio.sleep(5)

    async def _get_session(self):
        while self._work >= self._max_worker:
            await asyncio.sleep(0)

        self._work += 1

        await self._session_error.wait()

        if self._work == 1:
            await self._create_session()

        await self._session_update.wait()
        return self._session

    async def _close_session(self, code):
        self._work -= 1

        if code != 200 and self._work > 0 and self._session_error.is_set():
            self._session_error.clear()
            if self._session:
                await self._session[0].close()
            await self._create_session()

        if self._work == 0 and self._session:
            session, headers = self._session
            if code == 200:
                try:
                    await session.get(self._url + "/nttm-task-handler/api/logout", headers=headers, timeout=10)
                except Exception as err:
                    logger.error(f"Не удалось выйти из НТТМ: {err}")
            await session.close()
            self._session.clear()
            self._session_update.clear()

    async def _draft(self, method, url, timeout=10, params=None, data=None, json_=None):
        code, result = None, None
        request_data = {'method': method, 'url': url, 'timeout': timeout,
                        'params': params, 'data': data, 'json': json_}

        for _ in range(3):
            try:
                session, headers = await self._get_session()
                response = await session.request(**request_data, headers=headers)
                code = response.status

                if code == 200:
                    try:
                        result = await response.json()
                    except json.JSONDecodeError as err:
                        logger.warning(err)
                        result = await response.text()

                    await self._close_session(code)
                    break

                elif code == 400:
                    result = await response.text()
                    await self._close_session(code)
                    break
                
                else:
                    result = await response.text()
                    await self._close_session(code)
                    logger.error(result)
                    await asyncio.sleep(1)

            except Exception as err:
                code, result = 400, err
                logger.error(err, exc_info=True)
                await self._close_session(code)
                await asyncio.sleep(1)

        return code, result

    async def _get_tasks(self, task_id, filter_key):
        try:
            url_filter = f"{self._url}/nttm-user-profile/api/user-filters/{filter_key}"
            code, result = await self._draft(method='GET', url=url_filter)

            if code == 200:
                if not result:
                    code = 400
                    tasks = 'Custom error: filter None'

                else:
                    filter_ = result['filter'].encode('utf-8')
                    url_tasks = f"{self._url}/nttm-web-gateway/api/task/page"
                    params = {"page": "0",
                              "size": "100",
                              "sort": "id,desc"}
                    tasks = []
                    code, result = await self._draft(method='POST', url=url_tasks, timeout=90,
                                                     params=params, data=filter_)
                    if code == 200:
                        tasks.extend(result['content'])
                        if result['totalPages'] > 1:
                            for page in range(1, result['totalPages']):
                                params['page'] = str(int(params['page']) + 1)
                                code, result = await self._draft(method='POST', url=url_tasks, timeout=90,
                                                                 params=params, data=filter_)
                                if code == 200:
                                    tasks.extend(result['content'])
                                else:
                                    tasks = result
                                    break
                    else:
                        tasks = result
            else:
                tasks = result
            await self.results.put({'task_id': task_id, 'status_code': code, 'result': tasks})
        except Exception as err:
            logger.error(err, exc_info=True)
            await self.results.put({'task_id': task_id, 'status_code': 400, 'result': f'fatal error: {err}'})

    async def _get_incident(self, task_id, inc):
        try:
            url = f"{self._url}/nttm-web-gateway/api/ticket/{inc}"
            code, result = await self._draft('GET', url)
            await self.results.put({'task_id': task_id, 'status_code': code, 'result': result})
        except Exception as err:
            logger.error(err, exc_info=True)
            await self.results.put({'task_id': task_id, 'status_code': 400, 'result': f'fatal error: {err}'})

    async def _binding_network_inc(self, task_id, inc_si, inc_ki, skip_mrf='МРФ Центр'):
        """" Метод для привязки КИ к СИ

        Термины:
            task['id']: id задания в инциденте
            task['statusName']: В работе, В очереди
            task["taskExecutorDTO"]["execUnitId"] = id подразделения
            ruleId:  результат выполнения задания
            unitId:  целевое подразделение
            damageLevel: уровень повреждения
        """

        async def get_rules_id(task_id_):
            data_ = {}

            url = f"{self._url}/nttm-task-handler/api/tasks/{task_id_}/rules"
            code_, result_ = await self._draft('GET', url)

            if code_ == 200:
                for rules_ in result_:
                    data_[rules_["closeCode"]["name"]] = str(rules_["id"])
                return code_, data_
            return code_, result_

        async def get_inc(inc_):
            url = f"{self._url}/nttm-web-gateway/api/ticket/{inc_}"
            return await self._draft('GET', url)

        async def get_tasks(inc_ki_, new_task, old_task_id_=None, max_recursion=5):
            data = {
                "processNames": [],
                "coordinatorExecutor": [],
                "coordinatorSubprocess": [],
                "executorUnits": [],
                "dateFrom": [],
                "dateTo": []
            }
            url = f"{self._url}/nttm-task-handler/api/tasks/{inc_ki_}/filter"
            code_, result_ = await self._draft('POST', url, data=json.dumps(data))

            if code_ == 200:
                if new_task:
                    if max_recursion == 0:
                        code_, result_ = 500, 'New task not found'
                    elif int(result_[-1]['id']) == int(old_task_id_):
                        code_, result_ = await get_tasks(inc_ki_, True, old_task_id_, max_recursion=max_recursion - 1)

            return code_, result_

        async def get_unit_ids(nttm_task_id, rule_id_si):
            data_ = {}
            params = {"taskId": nttm_task_id, "ruleId": rule_id_si}
            url = f"{self._url}/nttm-task-handler/api/tools/units"
            code_, result_ = await self._draft('GET', url, params=params)

            if code_ == 200:
                for unitId in result_:
                    data_[unitId["name"]] = unitId["id"]
                return code_, data_
            return code_, result_

        async def accept_task(nttm_task_id, task_status):
            if task_status == 'В очереди':
                url = f"{self._url}/nttm-task-handler/api/tasks/{nttm_task_id}/assign"
            elif task_status == 'В работе':
                url = f"{self._url}/nttm-task-handler/api/tasks/{nttm_task_id}/fetch"
            else:
                return 400, f'Unknown status task {task_status}'

            code_, result_ = await self._draft('POST', url)
            return code_, result_

        async def close_task(task_id_, rule_id_, unit_id_, comment, add_data=None):
            payload = {
                "closeComment": comment,
                "closeGroupArr": [{
                    "ruleId": rule_id_,
                    "unitId": unit_id_,
                    "damageLevel": None
                }]}
            if add_data:
                payload.update(add_data)
            url = f"{self._url}/nttm-web-gateway/api/task/{task_id_}/close"
            code_, result_ = await self._draft('PUT', url, data=json.dumps(payload))
            # await asyncio.sleep(3)
            return code_, result_

        try:
            error = False
            # Шаг 1: Проверяем сетевой инцидент
            code, inc_si_data = await get_inc(inc_si)
            if code == 200:
                if inc_si_data['status'] == 'Закрыт':
                    code = 403
                    error = {'level': 'warning', 'step': '1: get_inc', 'err': 'Сетевой инцидент закрыт'}
                comment_si = inc_si_data['tasks'][-2]["closeComment"]
            else:
                error = {'level': 'warning', 'step': '1: get_inc', 'err': inc_si_data}

            # Шаг 1.1: получаем задания из инцидента
            if not error:
                code, inc_ki_data = await get_inc(inc_ki)
                if code == 200:
                    exec_unit_id_creator = inc_ki_data['tasks'][0]["taskExecutorDTO"]["execUnitId"]
                    task_end = inc_ki_data['tasks'][-1]
                else:
                    error = {'level': 'warning', 'step': '1.1: get_inc', 'err': inc_ki_data}

            # Шаг 1.2: проверяем и собираем данные таска
            if not error:
                skip = inc_ki_data['order']['selectedMrf'] not in skip_mrf
                check = [inc_ki_data['tasks'][-1]['taskExecutorDTO']["execUnitName"] == 'ДЭФИР ЛЦК ВАТС',
                         inc_ki_data['tasks'][-1]["typeName"] == 'Диагностика']
                if not all(check):
                    code = 406
                    description = ''
                    if not check[0]:
                        description += 'группа не ДЭФИР ЛЦК ВАТС'
                    if not check[0]:
                        description += '| тип не Диагностика'
                    error = {'level': 'warning', 'step': '1.2: check', 'err': f'В последнем задании: {description}'}

            # Шаг 2: принимаем текущее задание в работу
            if not error:
                code, result = await accept_task(task_end['id'], task_end['statusName'])
                if code != 200:
                    error = {'level': 'critical', 'step': '2: accept_task', 'err': result}

            # Шаг 3: получаем rule id
            if not error:
                code, rule_ids = await get_rules_id(task_end['id'])
                if code != 200:
                    error = {'level': 'critical', 'step': '3: get_rules_id', 'err': rule_ids}

            # Пропускаем шаги если инициатор МРФ Центр
            if not error and skip:

                # Шаг 4: Закрываем текущее задание
                if not error:
                    rule = rule_ids.get('Запрос направлен ошибочно')
                    if not rule:
                        error = {'level': 'critical', 'step': '4: close_task',
                                 'err': 'Not rules: Запрос направлен ошибочно'}
                    else:
                        comment = comment_si + "Привязка ТТ к сетевому инциденту была выполнена средствами VATS Manager Bot"
                        code, result = await close_task(task_end['id'], rule_ids['Запрос направлен ошибочно'],
                                                        exec_unit_id_creator, comment)
                        if code != 200:
                            error = {'level': 'critical', 'step': '4: close_task', 'err': result}

                # Шаг 5: Получаем новые задания
                if not error:
                    code, tasks = await get_inc(inc_ki)
                    if code == 200:
                        exec_unit_id_creator = inc_ki_data['tasks'][0]["taskExecutorDTO"]["execUnitId"]
                        task_end = inc_ki_data['tasks'][-1]
                    else:
                        error = {'level': 'critical', 'step': '5: get_tasks', 'err': tasks}

                # Шаг 6: получаем rule id
                if not error:
                    code, rule_ids = await get_rules_id(task_end['id'])
                    if code == 200:
                        rule = rule_ids.get('Связать с СИ')
                        if not rule:
                            error = {'level': 'warning', 'step': '6: get_rules_id',
                                     'err': 'Not rules: Связать с СИ'}
                    else:
                        error = {'level': 'warning', 'step': '6: get_rules_id', 'err': rule_ids}

                # Шаг 7: принимаем текущее задание в работу
                if not error:
                    code, result = await accept_task(task_end['id'], task_end['statusName'])
                    if code != 200:
                        error = {'level': 'critical', 'step': '7: accept_task', 'err': result}

            # Шаг 8: Получаем unit id привязки сетевого инцидента
            if not error:
                code, unit_ids = await get_unit_ids(task_end['id'], rule_ids['Связать с СИ'])
                if code != 200:

                    error = {'level': 'critical', 'step': '8: get_unit_id_si', 'err': unit_ids}

            # Шаг 9: Закрываем текущее задание (Привязываем к сетевому)
            if not error:
                rule = rule_ids.get('Связать с СИ')
                unit = unit_ids.get('Решение базового ТТ')
                if not rule or not unit:
                    error = {'level': 'warning', 'step': '9: get_rules_id',
                             'err': f'Not data, rule: {rule}, unit: {unit}'}
                else:
                    comment = comment_si + "Привязка ТТ к сетевому инциденту была выполнена средствами VATS Manager Bot"
                    data = {'networkTtId': str(inc_si)}
                    code, result = await close_task(task_end['id'], rule, unit, comment, add_data=data)
                    if code != 200:
                        error = {'level': 'critical', 'step': '9: close_task', 'err': result}

            # Шаг 10: Отправляем результат
            if error:
                await self.results.put({'task_id': task_id, 'status_code': code, 'result': error})
            else:
                await self.results.put({'task_id': task_id, 'status_code': code, 'result': f'{inc_ki} --> {inc_si} ok'})

        except Exception as err:
            logger.error(err, exc_info=True)
            error = {'level': 'fatal', 'step': '?', 'err': str(err)}
            await self.results.put({'task_id': task_id, 'status_code': 400, 'result': error})

    async def _core(self):
        task = {'task_id': None}

        while True:
            try:
                task = await self.tasks.get()

                if task.get('type') == 'get_tasks':
                    self._loop.create_task(self._get_tasks(task['task_id'], task['filter']))
                elif task.get('type') == 'get_inc':
                    self._loop.create_task(self._get_incident(task['task_id'], task['inc']))
                elif task.get('type') == 'binding_network_inc':
                    self._loop.create_task(self._binding_network_inc(task['task_id'], task['inc_si'], task['inc_ki']))
                else:
                    result = {'type': 'event', 'message': f'task type ({task.get("type")}) not found',
                              'task_id': task['task_id'], 'status_code': None}
                    await self.results.put(result)

            except Exception as err:
                logging.error(err, exc_info=True)
                await self.results.put({'type': 'event', 'message': err,
                                        'task_id': task['task_id']})


async def main():
    def sha256(data):
        hash_object = hashlib.sha256(bytes(False + data + False, encoding='utf-8'))
        hex_dig = hash_object.hexdigest()
        return hex_dig

    # Отправляет результаты
    async def sender_analyst(ws, nttm):
        while ws.open:
            result = json.dumps(await nttm.results.get())
            await ws.send(result)
            logger.debug(f"{ws.remote_address} sending: {str(result)[:50]}")

    while True:
        try:
            async with websockets.connect(False) as ws:
                # Авторизация
                await ws.send(False)
                salt = await ws.recv()
                await ws.send(sha256(salt))
                await ws.send('nttm')

                logger.info('ws client connecting')
                loop = asyncio.get_event_loop()
                nttm = ConnectorNTTM(loop)
                loop.create_task(sender_analyst(ws, nttm))

                # Получаем задания
                async for message in ws:
                    mes = json.loads(message)
                    logger.debug(f"{ws.remote_address} recv: {mes}")
                    await nttm.tasks.put(mes)

        except Exception as err:
            logging.error(err)
            await asyncio.sleep(3)


asyncio.run(main())
