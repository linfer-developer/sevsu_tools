import asyncio
from typing import Optional
from database.interface import *


class ImportQueues:
    """
    Класс `ImportQueues` реализует асинхронную очередь для обработки данных
    по неделям, группам и урокам. Обеспечивает запуск, остановку, добавление
    элементов и обработку очередей в асинхронном режиме.

    Атрибуты:
        week_queue (asyncio.Queue): Очередь для данных по неделям.
        group_queue (asyncio.Queue): Очередь для данных по группам.
        lesson_queue (asyncio.Queue): Очередь для данных по урокам.
        week_task (Optional[asyncio.Task]): Фоновая задача обработки недели.
        group_task (Optional[asyncio.Task]): Фоновая задача обработки групп.
        lesson_task (Optional[asyncio.Task]): Фоновая задача обработки уроков.

    Методы:
        start() -> None
        wait() -> None
        stop() -> None
        put_week(data: object) -> None
        put_group(data: object) -> None
        put_lesson(data: object) -> None
        import_week() -> None (асинхронный)
        import_group() -> None (асинхронный)
        import_lesson() -> None (асинхронный)
    """

    week_queue: asyncio.Queue = asyncio.Queue()
    group_queue: asyncio.Queue = asyncio.Queue()
    lesson_queue: asyncio.Queue = asyncio.Queue()
    week_task: Optional[asyncio.Task] = None
    group_task: Optional[asyncio.Task] = None
    lesson_task: Optional[asyncio.Task] = None

    @classmethod
    async def start(cls):
        """
        Запускает обработку очередей, создавая асинхронные задачи для каждого вида данных.

        :raises RuntimeError: Если задачи уже запущены.
        :rtype: None
        """
        cls.week_task = asyncio.create_task(cls.import_week())
        cls.group_task = asyncio.create_task(cls.import_group())
        cls.lesson_task = asyncio.create_task(cls.import_lesson())

    @classmethod
    async def wait(cls):
        """
        Ожидает завершения всех запущенных задач обработки очередей.

        :rtype: None
        """
        await asyncio.gather(
            cls.week_task,
            cls.group_task,
            cls.lesson_task
        )

    @classmethod
    async def stop(cls):
        """
        Останавливает обработку очередей, посылая сигнал "STOP" в каждую очередь
        и ожидая завершения всех задач.

        :rtype: None
        """
        await cls.week_queue.put("STOP")
        await cls.group_queue.put("STOP")
        await cls.lesson_queue.put("STOP")
        await cls.wait()

    @classmethod
    async def put_week(cls, data):
        """
        Добавляет данные в очередь недель для последующей обработки.

        :param data: Данные, которые необходимо обработать. Тип зависит от реализации `add_week`.
        :type data: object
        :rtype: None
        """
        await cls.week_queue.put(data)

    @classmethod
    async def put_group(cls, data):
        """
        Добавляет данные в очередь групп для последующей обработки.

        :param data: Данные, которые необходимо обработать. Тип зависит от реализации `add_group`.
        :type data: object
        :rtype: None
        """
        await cls.group_queue.put(data)

    @classmethod
    async def put_lesson(cls, data):
        """
        Добавляет данные в очередь уроков для последующей обработки.

        :param data: Данные, которые необходимо обработать. Тип зависит от реализации `add_lesson`.
        :type data: object
        :rtype: None
        """
        await cls.lesson_queue.put(data)

    @classmethod
    async def import_week(cls):
        """
        Асинхронный метод обработки очереди недель. Постоянно извлекает
        элементы из очереди, обрабатывает их функцией `add_week`.
        Останавливается при получении сигнала "STOP".

        :rtype: None
        """
        while True:
            item = await cls.week_queue.get()
            if item == "STOP":
                break
            if not item:
                continue
            await add_week(item)

    @classmethod
    async def import_group(cls):
        """
        Асинхронный метод обработки очереди групп. Постоянно извлекает
        элементы из очереди, обрабатывает их функцией `add_group`.
        Останавливается при получении сигнала "STOP".

        :rtype: None
        """
        while True:
            item = await cls.group_queue.get()
            if item == "STOP":
                break
            if not item:
                continue
            await add_group(item)

    @classmethod
    async def import_lesson(cls):
        """
        Асинхронный метод обработки очереди уроков. Постоянно извлекает
        элементы из очереди, обрабатывает их функцией `add_lesson`.
        Останавливается при получении сигнала "STOP".

        :rtype: None
        """
        while True:
            item = await cls.lesson_queue.get()
            if item == "STOP":
                break
            if not item:
                continue
            await add_lesson(item)