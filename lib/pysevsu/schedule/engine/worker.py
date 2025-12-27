import asyncio
import aiohttp
import time

from typing import Coroutine
from typing import List
from typing import Dict
from typing import Any
from io import BytesIO

from ..core.web import Parser
from ..core.xls import ExcelFile
from ..core.xls import Worksheet
from ..utilites.logger import log

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession


class BatchCTE_exporter:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        batch_size: int = 100,
        max_concurrent_batches: int = 4
    ):
        self.session_factory = session_factory
        self.batch_size = batch_size
        self._semaphore = asyncio.Semaphore(max_concurrent_batches)

        self._weeks_buffer: List[Dict[str, Any]] = []
        self._groups_buffer: List[Dict[str, Any]] = []
        self._lessons_buffer: List[Dict[str, Any]] = []

        self._week_key_cache = set()
        self._group_key_cache = set()

        self._buffer_lock = asyncio.Lock()

    @staticmethod
    def _generate_week_temp_key(data: Dict[str, Any]) -> str:
        return f"{data['title']}|{data['start_date']}|{data['end_date']}"
    
    @staticmethod
    def _generate_group_temp_key(data: Dict[str, Any]) -> str:
        return data['name']
    
    @staticmethod
    def _get_week(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'year': data.get('year', '2025'),
            'semester': data.get('semester'),
            'title': data['week'],
            'start_date': str(data.get('start_date')),
            'end_date': str(data.get('end_date'))
        }
    
    @staticmethod
    def _get_group(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'name': data['group'],
            'course': data['course'],
            'institute': data['institute']
        }
    
    @staticmethod
    def _get_lesson(
        week_temp_key: str, 
        group_temp_key: str, 
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            'week_key': week_temp_key,
            'group_key': group_temp_key,
            'study_form': data.get('study_form'),
            'weekday': data.get('weekday'),
            'date': data.get('date'),
            'number': data.get('number'),
            'start_time': data.get('start_time'),
            'title': data.get('title'),
            'teacher': data.get('teacher'),
            'type_': data.get('type'),
            'classroom': data.get('classroom')
        }

    async def add(self, data: Dict[str, Any]) -> None:
        async with self._buffer_lock:
            week_data = self._get_week(data)
            week_temp_key = self._generate_week_temp_key(week_data)
            group_data = self._get_group(data)
            group_temp_key = self._generate_group_temp_key(group_data)

            if week_temp_key not in self._week_key_cache:
                self._weeks_buffer.append(week_data)
                self._week_key_cache.add(week_temp_key)

            if group_temp_key not in self._group_key_cache:
                self._groups_buffer.append(group_data)
                self._group_key_cache.add(group_temp_key)

            lesson_data = self._get_lesson(
                week_temp_key=week_temp_key,
                group_temp_key=group_temp_key,
                data=data
            )
            self._lessons_buffer.append(lesson_data)

        if len(self._lessons_buffer) >= self.batch_size:
            await self._flush_buffered_data()

    async def _flush_buffered_data(self) -> None:
        if not self._lessons_buffer:
            return
        
        async with self._buffer_lock:
            weeks_to_insert = self._weeks_buffer.copy()
            groups_to_insert = self._groups_buffer.copy()
            lessons_to_insert = self._lessons_buffer.copy()

            self._weeks_buffer.clear()
            self._groups_buffer.clear()
            self._lessons_buffer.clear()
            self._week_key_cache.clear()
            self._group_key_cache.clear()

        async with self.semaphore:
            await self._execute_cte_insertion(
                weeks_to_insert,
                groups_to_insert,
                lessons_to_insert
            )

    async def _execute_cte_insertion(
        self,
        weeks_data: List[Dict[str, Any]],
        groups_data: List[Dict[str, Any]],
        lessons_data: List[Dict[str, Any]]
    ) -> None:
        if not lessons_data:
            return
        
        week_cte = self._build_week_cte(weeks_data)
        group_cte = self._build_group_cte(groups_data)
        insert_query = self._build_final_insert_query(
            week_cte,
            group_cte,
            lessons_data
        )

        async with self.session_factory() as session:
            async with session.begin():
                try:
                    await session.execute(insert_query)
                except Exception as e:
                    ... # TODO: Блокировка транзакций БД, исправить

    def _build_week_cte(self, weeks_data: List[Dict[str, Any]]) -> str:
        if not weeks_data:
            return """
            week_ids AS (
                SELECT NULL::bigint as id, NULL::text as week_key WHERE FALSE
            )
            """
        
        values_clause = ", ".join([
            f"('{w['year']}', '{w['semester']}', '{w['title']}', "
            f"'{w['start_date']}', '{w['end_date']}')"
            for w in weeks_data
        ])
        unique_key = "title, start_date, end_date"

        return f"""
        week_ids AS (
            WITH input_weeks(year, semester, title, start_date, end_date) AS (
                VALUES {values_clause}
            )
            INSERT INTO week (year, semester, title, start_date, end_date)
            SELECT iw.year, iw.semester, iw.title, iw.start_date, iw.end_date 
            FROM input_weeks iw
            ON CONFLICT ({unique_key}) DO UPDATE SET
                year = EXCLUDED.year
            RETURNING id, 
                CONCAT(title, '|', start_date, '|', end_date) as week_key
        )
        """
    
    def _build_group_cte(self, groups_data: List[Dict[str, Any]]) -> str:
        if not groups_data:
            return """group_ids AS (
                SELECT NULL::bigint as id, NULL::text as group_key WHERE FALSE
            )"""

        values_clause = ", ".join([
            f"('{g['name']}', '{g['course']}', '{g['institute']}')"
            for g in groups_data
        ])
        unique_key = "name"

        return f"""
        group_ids AS (
            WITH input_groups(name, course, institute) AS (
                VALUES {values_clause}
            )
            INSERT INTO "group" (name, course, institute)
            SELECT ig.name, ig.course, ig.institute FROM input_groups ig
            ON CONFLICT ({unique_key}) DO UPDATE SET 
                institute = EXCLUDED.institute
            RETURNING id, name as group_key
        )
        """

    def _build_final_insert_query(
        self, 
        week_cte: str, 
        group_cte: str, 
        lessons_data: List[Dict[str, Any]]
    ) -> str:
        lesson_values = []
        for lesson in lessons_data:
            values = (
                f"("
                f"(SELECT id FROM week_ids WHERE week_key = '{lesson['week_key']}'), "
                f"(SELECT id FROM group_ids WHERE group_key = '{lesson['group_key']}'), "
                f"'{lesson.get('study_form', '')}', "
                f"'{lesson.get('weekday', '')}', "
                f"'{lesson.get('date', '')}', "
                f"{lesson.get('number', 0)}, "
                f"'{lesson.get('start_time', '').replace(':', '-')}', "
                f"'{lesson.get('title', '').replace(':', '-')}', "
                f"'{lesson.get('teacher', '')}', "
                f"'{lesson.get('type_', '')}', "
                f"'{lesson.get('classroom', '')}'"
                f")"
            )
            lesson_values.append(values)

        values_clause = ", ".join(lesson_values)
        unique_key = "week_id, group_id, study_form, weekday, date, number, start_time, title, teacher, type_, classroom"

        return text(f"""
            WITH 
            {week_cte},
            {group_cte}
            INSERT INTO lesson
                (week_id, group_id, study_form, weekday, date, number, 
                start_time, title, teacher, type_, classroom)
            VALUES {values_clause}
            ON CONFLICT ({unique_key}) DO NOTHING
        """)

    async def finalize(self) -> None:
        await self._flush_buffered_data()


class Engine:
    def __init__(
        self, 
        max_request_count: int = 40,
        db_url: str = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule",
        db_pool_size: int = 20,
        db_max_overflow: int = 40, 
        db_sqlalchemy_echo: bool = False,
        db_import_batch_size: int = 600,
        db_max_concurrent_batches: int = 2
    ) -> None:
        self._requests_semaphore = asyncio.Semaphore(max_request_count)
        self._requests_session: object = ...
        self._exporter = BatchCTE_exporter(
            session_factory=async_sessionmaker(
                bind=create_async_engine(
                    url=db_url,
                    pool_size=db_pool_size,
                    max_overflow=db_max_overflow,
                    echo=db_sqlalchemy_echo
                ),
                expire_on_commit=False
            ),
            batch_size=db_import_batch_size,
            max_concurrent_batches=db_max_concurrent_batches
        )

    @log
    def start(self) -> None:
        while True:
            asyncio.run(self._run_parser())
            time.sleep(60*60*2) # TODO: Временное решение

    async def _run_parser(self) -> None:
        async with aiohttp.ClientSession() as self._requests_session:
            tasks: List[Coroutine] = list()
            web = Parser()
            async for i in web.run_data_stream():
                task = asyncio.create_task(
                    self._run_xls_files_headler(i.copy())
                )
                tasks.append(task)

            await asyncio.gather(*tasks)
            await self._exporter.finalize()

    async def _get_xls_file(self, end_url: str) -> None:
        url: str = rf"https://www.sevsu.ru{end_url}"
        try:
            async with self._requests_session.get(url) as response:
                if response.status == 200:
                    response.raise_for_status()
                    xls_content = BytesIO(await response.read())
                    return ExcelFile(xls_content)
                else:
                    ... # TODO: DLE
        except aiohttp.client_exceptions.ClientPayloadError: 
            ... # TODO: DLE
        except: 
            ... # TODO: Проанализировать отличные ошибки от ClientPayloadError

    async def _run_xls_files_headler(self, data: Dict[Any, Any]) -> None:
        async with self._requests_semaphore:
            xls: ExcelFile = await self._get_xls_file(data["excel_url"])
        if not xls:
            return None

        tasks: List[Coroutine] = list()
        async for sheet in xls.run_worksheets_stream():
            data["week"] = sheet.title
            data.update(sheet.get_dates_of_the_week())

            task = asyncio.create_task(
                self._run_worksheet_hander(sheet, data.copy())
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def _run_worksheet_hander(
        self, 
        xls_sheet: Worksheet,
        data: Dict[Any, Any]
    ) -> None:
        async for i in xls_sheet.run_data_stream():
            data.update(i)
            await self._exporter.add(data)


if __name__ == "__main__":
    engine: Engine = Engine()
    engine.start()