import asyncio
from typing import Callable, Any, Optional

class TaskQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.worker_task = None
        self.running = False

    async def _worker(self):
        while self.running:
            func, args, kwargs = await self.queue.get()
            try:
                result = await func(*args, **kwargs)
                # Можно обработать результат или логировать
            except Exception as e:
                # Обработка ошибок, например логирование
                print(f"Ошибка при выполнении задачи: {e}")
            finally:
                self.queue.task_done()

    def start(self):
        if not self.running:
            self.running = True
            self.worker_task = asyncio.create_task(self._worker())

    def stop(self):
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()

    async def add_task(self, coro: Callable, *args, **kwargs):
        await self.queue.put((coro, args, kwargs))

    async def add_tasks(self, func: Callable, *args, **kwargs):
        if isinstance(func, list):
            for coro in func:
                await self.queue.put((coro, args, kwargs))