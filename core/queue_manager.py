import asyncio
from typing import Optional
from database.interface import *


class ImportQueues:

    week_queue: asyncio.Queue = asyncio.Queue()
    group_queue: asyncio.Queue = asyncio.Queue()
    lesson_queue: asyncio.Queue = asyncio.Queue()

    week_task: Optional[asyncio.Task] = None
    group_task: Optional[asyncio.Task] = None
    lesson_task: Optional[asyncio.Task] = None

    @classmethod
    async def start(cls):
        cls.week_task = asyncio.create_task(cls.import_week())
        cls.group_task = asyncio.create_task(cls.import_group())
        cls.lesson_task = asyncio.create_task(cls.import_lesson())

    @classmethod
    async def wait(cls):
        await asyncio.gather(
            cls.week_task, 
            cls.group_task, 
            cls.lesson_task
        )

    @classmethod
    async def stop(cls):
        await cls.week_queue.put("STOP")
        await cls.group_queue.put("STOP")
        await cls.lesson_queue.put("STOP")
        await cls.wait()

    @classmethod
    async def put_week(cls, data):
        await cls.week_queue.put(data)

    @classmethod
    async def put_group(cls, data):
        await cls.group_queue.put(data)

    @classmethod
    async def put_lesson(cls, data):
        await cls.lesson_queue.put(data)

    @classmethod
    async def import_week(cls):
        while True:
            item = await cls.week_queue.get()
            if item == "STOP":
                break
            if not item:
                continue
            await add_week(item)

    @classmethod
    async def import_group(cls):
        while True:
            item = await cls.group_queue.get()
            if item == "STOP":
                break
            if not item:
                continue
            await add_group(item)

    @classmethod
    async def import_lesson(cls):
        while True:
            item = await cls.lesson_queue.get()
            if item == "STOP":
                break
            if not item:
                continue
            await add_lesson(item)
