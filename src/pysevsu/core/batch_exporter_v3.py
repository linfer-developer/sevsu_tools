import asyncio
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.pysevsu.models import tables
from src.pysevsu.utils import log


class _Cache:
    def __init__(self, max_size: int = -1):
        self.data: Set[Any] = set()
        self.max_size = max_size

    def add(self, hash_: Any) -> bool:
        if self.is_full():
            raise AttributeError("Размер кэша привышен")

        if hash_ and not self.contains(hash_):
            self.data.add(hash_)
            return True

        return False

    def contains(self, hash_: object) -> bool:
        return hash_ in self.data

    def add_all(self, *args: Any) -> None:
        for i in args:
            self.add(i)

    def is_full(self) -> bool:
        return len(self.data) > self.max_size and self.max_size != -1


class _Buffer:
    def __init__(self, max_size: int = -1):
        self.data: List[object] = []
        self.max_size = max_size

    def add_all(self, *objs: Tuple[object]):
        self.data.extend(objs)

    def add(self, obj: Any) -> None:
        if self.is_full():
            raise AttributeError(f"Размер буффера привышен: {self.max_size}, {self.data}")

        if obj:
            self.data.append(obj)
            return True
        else:
            return False

    def is_full(self) -> bool:
        return self.max_size != -1 and len(self.data) >= self.max_size

    def reset(self) -> None:
        self.data.clear()

    def sort(self) -> None:
        no_dependency = []
        has_dependency = []

        for obj in self.data:
            dependency = getattr(obj, '__dependencies__', None)
            if dependency is not None:
                has_dependency.append(obj)
            else:
                no_dependency.append(obj)

        no_dependency.sort(key=lambda x: type(x).__name__)
        has_dependency.sort(key=lambda x: type(x).__name__)

        self.data = no_dependency + has_dependency


class _IDManager:
    def __init__(self):
        self.data: Dict[str, int] = {}

    def get(self, hash_: str):
        return self.data.get(hash_)

    def remember_id(self, hash_: str, id_: int):
        self.data[hash_] = id_

    def remember_ids(self, objs, ids):
        i: int = 0
        for obj in objs:
            self.remember_id(obj.__hashcode__, ids[i])
            i += 1

    def assign_ids(self, obj: object) -> None:
        obj_vars: Dict[str, Any] = vars(obj)

        for var_name, var_value in obj_vars.items():
            if isinstance(var_value, str) and var_value.startswith("hash"):
                hash_value = var_value
                id_ = self.get(hash_value)

                if id_:
                    setattr(obj, var_name, id_)

    def assign_ids_all(self, *objs: List[object]):
        return [self.assign_ids(obj) for obj in objs]


class _SQLCodeBuilder:
    @staticmethod
    def build(*args: List[object]) -> str:
        first_obj: object = args[0]
        tablename: str = first_obj.__tablename__
        default_param: str = first_obj.__default_param__
        unique_keys: str = ", ".join(first_obj.__unique_keys__)
        fields: str = ", ".join(list(vars(first_obj).keys())[1:])
        tmp_values: List[str] = []

        for obj in args:
            if type(first_obj) is not type(obj):
                raise TypeError("")

            obj_values = list(vars(obj).values())
            value = _SQLCodeBuilder._transform_obj_values(obj_values)
            tmp_values.append(value)

        values: str = ",\n ".join(tmp_values)

        return f"""
            WITH "{tablename}_ids" AS (
                INSERT INTO "{tablename}" ({fields})
                VALUES {values}
                ON CONFLICT ({unique_keys}) DO UPDATE SET
                    {default_param} = EXCLUDED.{default_param}
                RETURNING id
            )
            SELECT id FROM {tablename}_ids
        """

    @staticmethod
    def _transform_obj_values(values: List[Any]) -> str:
        tmp_values: List = []
        standard_types = (int, str, float, bool)

        for value in values:
            if not isinstance(value, standard_types):
                continue

            if not value:
                value: str = "NULL"
            elif isinstance(value, str):
                value: str = f'\'{value.replace("$", " ")}\''

            value: str = str(value)
            tmp_values.append(value)

        return f"({', '.join(tmp_values)})"


class Distributor:
    def __init__(
        self,
        cache: _Cache = _Cache(1000000),
        buffer: _Buffer = _Buffer(10),
    ) -> None:
        self.cache = cache
        self.buffer = buffer

    async def stream(self, *objs: Optional[Tuple[object]]) -> iter:
        for obj in objs:
            hash_: hash = obj.__hashcode__

            if not self.cache.contains(hash_):
                self.buffer.add(obj)
                self.cache.add(hash_)

                if self.buffer.is_full():
                    self.buffer.sort()
                    yield self.buffer
                    self.buffer.reset()

    def add_remaining_data(self):
        if self.buffer.data:
            self.buffer.sort()
            self.buffer.reset()


class _Session:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.session_factory = session_factory

    async def execute(self, sql: str) -> Optional[List[int]]:
        sql_query = text(sql)

        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(sql_query)
                return [row[0] for row in result.fetchall()]


class Exporter:
    def __init__(
        self,
        session: _Session,
        id_manager: _IDManager,
        max_concurrent_batches: int = 1,
    ) -> None:
        self._session = session
        self._id_manager = id_manager
        self._limit = asyncio.Semaphore(max_concurrent_batches)

    async def execute(self, buffer: _Buffer) -> None:
        async with self._limit:
            await self._export_objects_various_types(buffer)

    async def _export_objects_various_types(self, buffer: _Buffer) -> None:
        previous_type: object = None
        objs: List[object] = []

        for obj in buffer.data:
            current_type = type(obj)

            if previous_type is not None and current_type != previous_type:
                await self._export(*objs)
                objs.clear()

            objs.append(obj)
            previous_type = current_type

        if objs:
            await self._export(*objs)

    async def _export(self, *objs: Optional[Tuple[object]]) -> None:
        dependencies: bool = True if objs[0].__dependencies__ else False
        print(objs)

        # if dependencies:
        #     self._id_manager.assign_ids_all(*objs)
        #     sql: str = _SQLCodeBuilder.build(*objs)
        #     ids: List[int] = await self._session.execute(sql)
        #     self._id_manager.remember_ids(objs, ids)
        # else:
        #     sql: str = _SQLCodeBuilder.build(*objs)
        #     ids: List[int] = await self._session.execute(sql)
        #     self._id_manager.remember_ids(objs, ids)


class Engine:
    def __init__(
        self,
        session_factory: object,
        cache_max_size: int = 2,
        buffer_max_size: int = 8,
        max_concurrent_batches: int = 1,
    ) -> None:
        session: _Session = _Session(session_factory)
        cache: _Cache = _Cache(cache_max_size)
        buffer: _Buffer = _Buffer(buffer_max_size)
        id_manager: _IDManager = _IDManager()

        self._exporter: Exporter = Exporter(session, id_manager, max_concurrent_batches)
        self._distributor: Distributor = Distributor(cache, buffer)

    async def execute(self, *objs: Tuple[object]):
        tasks: List = []

        async for buffer in self._distributor.stream(*objs):
            task = asyncio.create_task(self._exporter.execute(buffer))
            tasks.append(task)

        await asyncio.gather(*tasks)

