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
            raise AttributeError("Размер буффера привышен")

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


class DataStreamer:
    def __init__(
        self,
        max_cache_size: int = 8048,
        max_buffer_size: int = 10,
    ) -> None:
        self.cache = _Cache(max_cache_size)
        self.buffer = _Buffer(max_buffer_size)
        self.max_buffer_size = max_buffer_size
        self.lock = asyncio.Lock()

    def stream(self, *objs: Optional[Tuple[object]]) -> iter:
        if not objs: 
            return False

        for index, obj in enumerate(objs):
            hash_: hash = obj.__hashcode__

            if self.buffer.is_full():
                self.buffer.sort()
                yield self.buffer
                self.buffer.reset()

            if not self.cache.contains(hash_):
                self.cache.add(hash_)
                self.buffer.add(obj)


class _DatabaseSession:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.session_factory = session_factory

    async def execute_sql_with_id_return(self, sql: str) -> Optional[List[int]]:
        sql_query = text(sql)

        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(sql_query)
                return [row[0] for row in result.fetchall()]


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
        obj_copy: object = obj.copy()

        for var_name, var_value in obj_vars.items():
            if isinstance(var_value, str) and var_value.startswith("hash"):
                hash_value = var_value
                id_ = self.get(hash_value)

                if id_:
                    setattr(obj_copy, var_name, id_)

        return obj_copy

    def assign_ids_all(self, *objs: List[object]):
        buffer: List[object] = list()

        for obj in objs:
            buffer.append(self.assign_ids(obj))

        return buffer


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

        # TODO: Лучше использовать автоматически присваимые типы алхимии в standart_types

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


class Exporter:
    def __init__(self, db_session, id_manager):
        self.db_session = db_session
        self.id_manager = id_manager

    def execute(self, buffer: _Buffer) -> iter:
        previous_type: object = None
        objs: List[object] = []

        for obj in buffer.data:
            current_type = type(obj)

            if previous_type is not None and current_type != previous_type:
                self._write(*objs)
                objs.clear()

            objs.append(obj)
            previous_type = current_type

        if objs:
            self._write(*objs)

    async def _write(self, *objs: Optional[Tuple[object]]):
        dependencies: bool = True if objs[0].__dependencies__ else False

        if dependencies:
            assign_id_objs = self.id_manager.assign_ids_all(*objs)
            sql: str = _SQLCodeBuilder.build(*assign_id_objs)
            ids: List[int] = await self.db_session.execute_sql_with_id_return(sql)
            self.id_manager.remember_ids(assign_id_objs, ids)
        else:
            sql: str = _SQLCodeBuilder.build(*objs)
            ids: List[int] = await self.db_session.execute_sql_with_id_return(sql)
            self.id_manager.remember_ids(objs, ids)
