import random
import hashlib
import threading
import json
import time
import asyncio
import websockets
import queue
from log import logger


class ConnectorService:
    def __init__(self):
        self.clients = []
        self._queue_tasks = queue.Queue()
        self._complete_tasks = {}
        self._max_id = 100
        self._end_id = 0
        self._lock = threading.RLock()

    async def _send(self):
        ws = None
        while True:
            try:
                if self.clients:
                    task = self._queue_tasks.get(False)
                    ws = self.clients[0]
                    logger.debug(f"{ws.remote_address} sending: {task}")
                    await ws.send(json.dumps(task))
                await asyncio.sleep(0.01)

            except queue.Empty:
                await asyncio.sleep(0.01)
            except Exception:
                await self._disconnect(ws)

    async def _recv(self):
        ws = None
        while True:
            try:
                for ws in self.clients:
                    message = await asyncio.wait_for(ws.recv(), timeout=0.01)
                    if message:
                        message = json.loads(message)
                        self._complete_tasks[message.pop('task_id')] = message
                        logger.debug(f"{ws.remote_address} recv: {str(message)[:400]}...")

                await asyncio.sleep(0.01)

            except asyncio.TimeoutError:
                await asyncio.sleep(0.01)
            except Exception as err:
                await self._disconnect(ws)

    async def _disconnect(self, ws):
        if ws in self.clients:
            if ws.open:
                await ws.close()
            self.clients.remove(ws)
            logger.warning(f"client {ws.service} {ws.remote_address} disconnect")

    def run(self):
        asyncio.create_task(self._send())
        asyncio.create_task(self._recv())

    def check_client(self):
        if len(self.clients) > 0:
            return True
        return False

    def create_task(self, task: dict):
        if len(self.clients) == 0:
            return {'status_code': 501, 'result': 'module not connect'}
            
        with self._lock:
            if self._end_id < self._max_id:
                task_id = self._end_id + 1
                self._end_id = task_id
            else:
                task_id = 1
                self._end_id = 1

            task.update(task_id=task_id)
            self._queue_tasks.put(task)

        timeout = 600
        limit_time = timeout + time.time()

        while True:
            with self._lock:
                if task_id in self._complete_tasks:
                    return self._complete_tasks.pop(task_id)

            if limit_time < time.time():
                text = f'Задание ({task}) не выполнено: timeout error, waiting for more than {timeout} seconds'
                logger.error(text)
                return {'status_code': 500, 'result': text}
            time.sleep(0.01)


class ConnectorNTTM(ConnectorService):
    def get_tasks(self, key_filter=None):
        if not key_filter:
            key_filter = '18411687-e2ab-4666-ac9f-f27dc14fe0bf'
        task = {'type': 'get_tasks', 'filter': key_filter}
        return self.create_task(task)

    def get_inc(self, inc):
        task = {'type': 'get_inc', 'inc': str(inc)}
        return self.create_task(task)

    def binding_network_inc(self, inc_si, inc_ki):
        task = {'type': 'binding_network_inc', 'inc_si': str(inc_si), 'inc_ki': str(inc_ki)}
        return self.create_task(task)


class ConnectorVMSP(ConnectorService):
    def keep_alive(self):
        task = {'type': 'keep_alive_request'}
        return self.create_task(task)

    def send_register(self, peer):
        task = {'type': 'send_retry_register', 'peer': peer}
        return self.create_task(task)


class ServerServices:
    """
    Подключение сервиса происходит:
    1. От сервиса ожидается ТОКЕН
    2. Сервису передается соль
    3. От сервиса ожидается хеш (sha256: токен+соль+ключ)
    4. От сервиса ожидается название сервиса (такое же как свойство класса, например, nttm (т.к. self.nttm) или skuf)
    """

    def __init__(self, port):
        self._port = port
        self._TOKEN = False
        self._KEY = False
        self.nttm = ConnectorNTTM()
        self.skuf = ConnectorService()
        self.vmsp = ConnectorVMSP()
        threading.Thread(target=lambda: asyncio.run(self.create_server()), daemon=True).start()

    async def create_server(self):
        self.nttm.run()
        self.skuf.run()
        self.vmsp.run()

        logger.info("webserver start")
        async with websockets.serve(self._authentication, port=self._port, max_size=10240000):
            await asyncio.Future()  # run forever

    async def _authentication(self, ws):
        try:
            message = await asyncio.wait_for(ws.recv(), timeout=2)

            if message == self._TOKEN:
                r_number = str(random.randint(1000, 1000000))
                await ws.send(r_number)

                hash_object = hashlib.sha256(bytes(self._TOKEN + r_number + self._KEY, encoding='utf-8'))
                hex_dig = hash_object.hexdigest()
                message = await asyncio.wait_for(ws.recv(), timeout=2)

                if hex_dig == message:
                    service_name = await asyncio.wait_for(ws.recv(), timeout=2)
                    
                    if service_name in self.__dict__:
                        service = self.__getattribute__(service_name)
                        ws.service = service_name
                        logger.info(f"client {ws.service} {ws.remote_address} connect")
                        service.clients.append(ws)
                        await ws.wait_closed()

                        if ws in service.clients:
                            service.clients.remove(ws)
                            logger.warning(f"client {ws.service} {ws.remote_address} disconnect")
                    else:
                        await ws.close(code=4002, reason='Service is not supported')
                else:
                    await ws.close(code=4001, reason='Unauthorized')
            else:
                await ws.close(code=4001, reason='Unauthorized')
        except Exception as err:
            logger.info(f"client {ws.remote_address} not auth, err {err}")


services = ServerServices(80)

