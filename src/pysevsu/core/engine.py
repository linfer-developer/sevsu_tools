"""transfer.py

The transfer module provides the main data processing pipeline for collecting,
parsing, and storing schedule data from Sevastopol State University. It
orchestrates the workflow from web scraping Excel files to database export.

This module serves as the core integration layer between web scraping (web.py),
Excel parsing (excel.py), and database operations. It should only be used
within the core package.

TODO: Update documentation to match project standards.
"""

import asyncio
import json
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Coroutine, Dict, List, Optional, Set, Tuple, Type

import aiohttp
from sqlalchemy import Table, UniqueConstraint, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)

from src.pysevsu.core import batch_exporter, web, xls
from src.pysevsu.models import tables
from src.pysevsu.utils import log


class Produser:
    def __init__(
        self,
        queue: asyncio.Queue,
        request_limit: int = 5,
    ) -> None:
        self._queue = queue
        self._requests_limit_value = request_limit
        self._requests_session: aiohttp.ClientSession = None
        self._requests_limit: asyncio.Semaphore = None

    def run(self):
        asyncio.run(self._parse_files_to_website())

    async def async_run(self):
        await self._parse_files_to_website()

    async def _parse_files_to_website(self) -> None:
        self._requests_limit = asyncio.Semaphore(self._requests_limit_value)
        content: str = await web.get_website_content()
        doc: web.Document = web.Document(content)

        async with aiohttp.ClientSession() as self._requests_session:
            tasks: List[Coroutine] = []

            async for data in doc.parse_all():
                if not data[web.DataFields.filename].startswith("Распоряжение"):
                    tmp_data: Dict[str, str] = data.copy()
                    xls_file_end_url: str = data[web.DataFields.file_url]
                    task: Coroutine = asyncio.create_task(
                        self._handle_xls_file(tmp_data, xls_file_end_url)
                    )
                    tasks.append(task)

            await asyncio.gather(*tasks)

    async def _get_xls_file(self, end_url: str) -> None:
        url: str = rf"https://www.sevsu.ru{end_url}"

        try:
            async with self._requests_session.get(url) as response:
                response.raise_for_status()
                bin_file: BytesIO = BytesIO(await response.read())

            return xls.ExcelFile(bin_file)
        except aiohttp.client_exceptions.ClientPayloadError as e:
            print(e)
        except Exception as e:
            print(e)

    async def _handle_xls_file(
        self, tmp_data: Dict[str, Any], excel_file_url: str
    ) -> None:
        async with self._requests_limit:
            file: xls.ExcelFile = await self._get_xls_file(excel_file_url)

        if not file:
            return

        tasks: List[Coroutine] = []

        async for sheet in file.run_worksheets_stream():
            week_info: Dict[str, str] = sheet.get_week_info()
            tmp_data.update(week_info)
            task: Coroutine = asyncio.create_task(
                self._handle_worksheet_handler(sheet, tmp_data.copy())
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def _handle_worksheet_handler(
        self, xls_sheet: xls.Worksheet, tmp_data: Dict[str, Any]
    ) -> None:
        async for data in xls_sheet.run_data_stream():
            tmp_data.update(data)
            objs: Tuple[object] = _create_data_for_export(tmp_data)

            for obj in objs:
                await self._queue.put(obj)


STOP: Any = "STOP"


class Consumer:
    def __init__(
        self,
        queue: asyncio.Queue,
        session: object,
        max_concurrent_batches = 1,
        batch_size: int = 20,
        cache_size: int = 200000,
    ) -> None:
        self.session = session
        self.queue = queue
        self.batch_size = batch_size
        self.cache_size = cache_size
        self.max_concurrent_batches = max_concurrent_batches
        self.cache: Dict[str, int] = {}

    async def stream(self) -> None:
        graph = batch_exporter._GraphOfTableDependencies(20)

        while True:
            obj = await self.queue.get()
            if obj == STOP:
                break
            
            model: batch_exporter._Model = _Model(obj)
            print(graph.add(model))

    async def stream2(self) -> None:
        args: List[object] = set()
        tasks: List[Coroutine] = []
        query_limit: asyncio.Semaphore = asyncio.Semaphore(
            self.max_concurrent_batches
        )

        while True:
            obj = await self.queue.get()
            if obj == STOP:
                break

            size: int = len(args)
            if size >= self.batch_size:
                kw: List[object] = Consumer.distribute(*args)
                async with query_limit:
                    await self.export(**kw)
                args.clear()

            args.add(obj)
            self.queue.task_done()

        if args:
            kw: List[object] = Consumer.distribute(*args)
            async with query_limit:
                await self.export(**kw)

        await asyncio.gather(*tasks)

    @staticmethod
    def distribute(*args: object) -> None:
        if not args:
            raise AttributeError("Soon")

        has_relationships: List[object] = []
        no_relationships: List[object] = []
        _cache: Set[object] = set()

        for obj in args:
            obj = _Model(obj)
            if obj.key not in _cache:
                _cache.add(obj.key)
                if obj.has_relationship:
                    has_relationships.append(obj)
                else:
                    no_relationships.append(obj)

        has_relationships.sort(key=lambda obj: type(obj).__name__)
        no_relationships.sort(key=lambda obj: type(obj).__name__)

        return {
            "no_relationships": no_relationships,
            "has_relationships": has_relationships
        }

    async def export(self, **kw: object) -> None:
        if not kw:
            raise AttributeError("Soon")

        no_relationships: List[object] = kw["no_relationships"]
        has_relationships: List[object] = kw["has_relationships"]
        _ids: Dict[str, int] = {}

        async with self.session() as session:
            async with session.begin():
                async for query in _SQLQueryBuilder.build_query(*no_relationships):
                    stmt = query[0]
                    keys = query[1]
                    query = await session.execute(stmt)
                    result: List[int] = list(query)
                    _ids.update({
                        keys[index]: result[index]
                        for index, _ in enumerate[0](result)
                    })
                await session.flush()

                for obj in has_relationships:
                    for relationship in obj.relationships:
                        print(obj, relationship, _ids.get(obj.key))
                        setattr(obj, relationship, _ids.get(obj.key))

                async for query in _SQLQueryBuilder.build_query(*has_relationships):
                    stmt = query[0]
                    keys = query[1]
                    query = await session.execute(stmt)
                    result: List[int] = list(query)
                    _ids.update({
                        keys[index]: result[index]
                        for index, _ in enumerate[0](result)
                    })

class _Model:
    def __init__(self, obj: object) -> None:
        self.obj = obj
        self.model = obj.__class__
        self.table = self.model.__table__
        self._mapper = inspect(type(obj))

    @property
    def key(self) -> str:
        string: str = "|"
        vars_: vars = vars(self.obj).items()
        for key, item in vars_:
            if key in self.unique_fields:
                string += f"{str(item)}|"
        return string

    @property
    def has_relationship(self) -> bool:
        for column in self._mapper.columns:
            if column.foreign_keys and not column.nullable:
                return True
        return False

    @property
    def relationships(self) -> iter:
        for column in self._mapper.columns:
            if column.foreign_keys and not column.nullable:
                yield column.name

    @property
    def editable_fields(self) -> List[str]:
        editable: List[str] = []
        for column in self._mapper.columns:
            if not column.primary_key:
                editable.append(column.name)
        return editable

    @property
    def tablename(self) -> str:
        return self._mapper.local_table.name

    @property
    def unique_fields(self) -> List[str]:
        for constraint in self.table.constraints:
            if isinstance(constraint, UniqueConstraint):
                return {col.name for col in constraint.columns}
        return {col.name for col in self._mapper.columns if col.unique}

    @property
    def record_instances(self) -> Dict[str, Any]:
        record_instances = dict(vars(self.obj))
        del record_instances["_sa_instance_state"]
        return record_instances

    @property
    def any_not_unique_field(self) -> str:
        unique_fields: List[str] = self.unique_fields
        for field in self.record_instances.keys():
            if field not in unique_fields and field != "_sa_instance_state":
                return field

    @property
    def any_editable_field(self) -> str:
        for column in self._mapper.columns:
            if not column.primary_key:
                return column.name

    def __hash__(self):
        return hash(self.key)


class _SQLQueryBuilder:
    @staticmethod
    async def build_query(*args: object) -> iter:
        if not args:
            raise AttributeError("Soon")

        first_obj: _Model = args[0]
        previous_tablename: object = first_obj.tablename
        args_for_build: List[object] = []

        for obj in args:
            current_tablename: str = obj.tablename
            if current_tablename != previous_tablename:
                yield _SQLQueryBuilder.build(*args_for_build)
                args_for_build.clear()
            args_for_build.append(obj)
            previous_tablename = current_tablename

        if args_for_build:
            yield _SQLQueryBuilder.build(*args_for_build)

    @staticmethod
    def build(*args: _Model) -> insert:
        if not args:
            raise AttributeError("Не передан args")

        first_object: _Model = args[0]
        table: Table = first_object.table
        unique_fields: List[str] = first_object.unique_fields
        values: List[Dict[str, Any]] = [
            obj.record_instances
            for obj in args
        ]
        update_field: str = first_object.any_editable_field
        values: List[Dict[str, Any]] = []
        keys: List[str] = []

        for obj in args:
            record_instances = obj.record_instances
            keys.append(obj.key)
            values.append(record_instances)

        if not unique_fields:
            stmt = insert(table).values(values)
            if table.primary_key.columns and table.primary_key.columns[0].name:
                stmt = stmt.returning(table.c.id)
            return stmt

        stmt = insert(table).values(values)
        return [
            stmt.on_conflict_do_update(
                index_elements=unique_fields,
                set_={update_field: getattr(stmt.excluded, update_field)},
            ).returning(table.c.id),
            keys
        ]


class Engine:
    def __init__(
        self,
        request_limit: int = 30,
        queue_size: int = -1,
        db_url: str = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule",
        db_pool_size: int = 20,
        db_max_overflow: int = 40,
        db_sqlalchemy_echo: bool = True,
    ) -> None:
        if queue_size == -1:
            self.queue = asyncio.Queue()
        else:
            self.queue = asyncio.Queue(maxsize=queue_size)

        session_factory: AsyncSession = async_sessionmaker(
            bind=create_async_engine(
                url=db_url,
                pool_size=db_pool_size,
                max_overflow=db_max_overflow,
                echo=db_sqlalchemy_echo,
            ),
            expire_on_commit=False,
        )

        self.produser = Produser(self.queue, request_limit)
        self.consumer = Consumer(self.queue, session_factory)

    async def start(self):
        produser_run = asyncio.create_task(self.produser.async_run())
        cunsomer_stream = asyncio.create_task(self.consumer.stream())

        await produser_run
        await self.queue.join()
        await self.queue.put("STOP")
        await cunsomer_stream


@dataclass(frozen=True)
class DataFields:
    hashcode: str = "hashcode"
    table_fields: str = "fields"
    service_information: str = "service_information"

    table_name: str = "table_name"
    unique_keys: str = "unique_keys"
    dependency: str = "dependency"

    week_year: str = "year"
    week_title: str = "title"
    week_start_date: str = "start_date"
    week_end_date: str = "end_date"
    week_semester: str = "semester"

    groupname: str = "group"
    group_full_course_name: str = "full_course_name"
    group_institute: str = "institute"

    lesson_study_form: str = "study_form"
    lesson_group_id: str = "group_id"
    lesson_week_id: str = "week_id"
    lesson_day: str = "weekday"
    lesson_date: str = "date"
    lesson_number: str = "number"
    lesson_start_time: str = "start_time"
    lesson_title: str = "title"
    lesson_teacher: str = "teacher"
    lesson_type: str = "type_"
    lesson_classroom: str = "classroom"


def _create_data_for_export(tmp: Dict[str, Any]) -> Tuple[object]:
    week_obj = tables.Week(
        year=tmp.get(xls.DataFields.week_year, ""),
        title=tmp.get(xls.DataFields.week_title, ""),
        start_date=str(tmp.get(xls.DataFields.week_start_date, "")),
        end_date=str(tmp.get(xls.DataFields.week_end_date, "")),
        semester=str(tmp.get(web.DataFields.inlist_title, "")),
    )
    group_obj = tables.Group(
        name=tmp.get(xls.DataFields.group, ""),
        course=tmp.get(web.DataFields.filename, ""),
        institute=tmp.get(web.DataFields.list_title, ""),
    )
    lesson_obj = tables.Lesson(
        study_form=tmp.get(web.DataFields.head, ""),
        week=week_obj,
        group=group_obj,
        weekday=tmp.get(xls.DataFields.day, ""),
        date=str(tmp.get(xls.DataFields.date, "")),
        number=tmp.get(xls.DataFields.number, ""),
        start_time=tmp.get(xls.DataFields.start_time, ""),
        title=tmp.get(xls.DataFields.title, ""),
        teacher=tmp.get(xls.DataFields.teacher, ""),
        type_=tmp.get(xls.DataFields.type_, ""),
        classroom=tmp.get(xls.DataFields.classroom, ""),
    )

    return week_obj, group_obj, lesson_obj


if __name__ == "__main__":
    test: Engine = Engine()
    asyncio.run(test.start())
