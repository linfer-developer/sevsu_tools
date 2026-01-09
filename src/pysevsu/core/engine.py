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
from typing import Any, Coroutine, Dict, List, Optional, Set

import aiohttp
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.pysevsu.core import web, xls
from src.pysevsu.models import tables
from src.pysevsu.utils import log


class BatchCTEExporter:
    """Batch exporter for efficiently inserting data using Common Table Expressions.

    This class handles batch processing and insertion of schedule data into
    the database using CTE-based upsert operations. It manages dependencies
    between related tables and prevents duplicate insertions through hash-based
    caching.

    :param session_factory: SQLAlchemy async session factory
    :param batch_size: Number of records to accumulate before database flush
    :param max_concurrent_batches: Maximum concurrent database operations
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        batch_size: Optional[int] = 10,
        max_concurrent_batches: Optional[int] = 1,
    ) -> None:
        self.session = session_factory
        self.batch_size = batch_size
        self.lock = asyncio.Lock()
        self.limit = asyncio.Semaphore(max_concurrent_batches)
        self.shared_buffer: Dict[str, Any] = {}
        self.cache: Set[Any] = set()
        self.counter: int = 0

    async def add(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Adds data to the export buffer and triggers flush when batch is full.

        Processes structured data dictionaries, extracts hashcodes for duplicate
        detection, and buffers data for batch insertion. Automatically triggers
        database flush when batch size threshold is reached.

        :param data: Dictionary containing table-structured data to export
        :raises: Database-related exceptions during flush operations
        """
        for tablename in data:
            tmp: Dict[str, Any] = data[tablename]
            service_information: Dict[str, Any] = tmp["service_information"]
            hashcode: hash = service_information["hashcode"]
            fields: Dict[str, Any] = tmp["fields"]
            fields_values: List[Any] = list(fields.values())

            if tablename not in self.shared_buffer:
                keys: List[str] = list(fields.keys())
                self.shared_buffer[tablename] = {
                    "service_information": service_information,
                    "keys": keys,
                    "values": [],
                }

            if hashcode not in self.cache:
                values: List[Any] = fields_values
                self.shared_buffer[tablename]["values"].append(values)
                self.cache.add(hashcode)
                self.counter += 1

        if self.counter >= self.batch_size:
            self.counter = 0
            await self._flush_buffered_data()

    async def _flush_buffered_data(self) -> None:
        """Flushes buffered data to the database in a thread-safe manner.

        Uses locking to ensure data consistency during concurrent operations
        and manages concurrent batch execution through semaphore limiting.
        """
        async with self.lock:
            items_to_insert = self.shared_buffer.copy()
            self.shared_buffer.clear()

        async with self.limit:
            await self._execute_cte_insertion(items_to_insert)

    async def _execute_cte_insertion(self, items_to_insert: str) -> None:
        """Executes CTE-based INSERT operations for buffered data.

        Constructs and executes complex CTE SQL statements that handle
        table dependencies and conflict resolution in a single transaction.

        :param items_to_insert: Processed data ready for database insertion
        """
        if not items_to_insert:
            return

        final_ctes: Set[str] = set()
        tmp_ctes: Set[str] = set()
        for tablename in items_to_insert:
            service_info = items_to_insert[tablename]["service_information"]
            items = items_to_insert[tablename]
            if service_info["dependency"]:
                sql: str = self._build_with_dependency(
                    tmp_ctes, tablename, service_info, items
                )
                final_ctes.add(sql)
            else:
                sql: str = self._build(tablename, service_info, items)
                tmp_ctes.add(sql)

        final_ctes = ", ".join(final_ctes)

    def _build(
        self,
        tablename: str,
        service_info: Dict[str, str],
        items: List[str],
    ) -> None:
        """Builds CTE SQL for tables without dependencies.

        Constructs INSERT ... ON CONFLICT DO UPDATE statements for independent
        tables, returning generated IDs for use in dependent tables.

        :param tablename: Target database table name
        :param service_info: Table metadata including unique constraints
        :param items: Data items to insert
        :return: Complete CTE SQL statement
        """
        if not items:
            return """
                ids AS (
                    SELECT NULL::bigint as id, NULL::text as key WHERE FALSE
                )
            """

        keys: List[str] = list(items["keys"])
        values: List[str] = list(items["values"])

        input_fields: str = ", ".join(keys)
        values_clause: str = ", ".join([f"({", ".join(value)})" for value in values])
        input_fields_with: str = ", ".join([f"input_item.{key}" for key in keys])
        unique_keys: str = ", ".join(service_info["unique_keys"])
        default_param: str = "year"
        concat: str = ", '|', ".join(keys)

        sql: str = f"""
            {tablename}_ids AS (
                WITH input_{tablename} ({input_fields}) AS (
                    VALUES {values_clause}
                )
                INSERT INTO {tablename} ({input_fields})
                SELECT {input_fields_with} FROM input_{tablename} input_item
                ON CONFLICT ({unique_keys}) DO UPDATE SET
                    {default_param} = EXCLUDED.{default_param}
                RETURNING id, CONCAT({concat}) as key
            )
        """
        return sql

    def _build_with_dependency(
        self,
        ctes: List[str],
        tablename: str,
        service_info: Dict[str, str],
        items: List[str],
    ) -> None:
        """Builds CTE SQL for tables with foreign key dependencies.

        Constructs INSERT statements that reference IDs from previously
        inserted rows in dependent tables, maintaining referential integrity.

        :param ctes: List of pre-built CTE statements for dependencies
        :param tablename: Target database table name
        :param service_info: Table metadata including dependency information
        :param items: Data items to insert
        :return: Complete CTE SQL statement with dependencies
        """
        for tablename in service_info["dependency"]:
            items["fields"][
                f"{tablename}_id"
            ] = f"(SELECT id FROM {tablename}_ids WHERE {tablename}_key = {service_info["dependency"][tablename]})"

        keys: List[str] = list(items["keys"])
        values: List[str] = list(items["values"])

        ctes: str = ", ".join(ctes)
        input_fields: str = ", ".join(keys)
        values_clause: str = ", ".join([f"({", ".join(value)})" for value in values])
        unique_keys: str = ", ".join(service_info["unique_keys"])

        sql: str = f"""
            WITH {ctes}
            INSERT INTO {tablename} ({input_fields})
            VALUES {values_clause}
            ON CONFLICT ({unique_keys}) DO NOTHING
        """
        print(sql)
        return sql


class GlobalDataTransfer:
    """Main orchestrator class for the entire data transfer pipeline.

    Manages the complete workflow from web scraping to database insertion,
    including rate limiting, concurrent processing, and error handling.

    :param request_limit: Maximum concurrent HTTP requests
    :param db_url: Database connection URL
    :param db_pool_size: Database connection pool size
    :param db_max_overflow: Maximum overflow connections
    :param db_sqlalchemy_echo: Enable SQLAlchemy query echoing
    :param db_import_batch_size: Batch size for database imports
    :param db_max_concurrent_batches: Maximum concurrent database batches
    """

    def __init__(
        self,
        request_limit: int = 55,
        db_url: str = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule",
        db_pool_size: int = 20,
        db_max_overflow: int = 40,
        db_sqlalchemy_echo: bool = False,
        db_import_batch_size: int = 600,
        db_max_concurrent_batches: int = 2,
    ) -> None:
        self.requests_limit = asyncio.Semaphore(request_limit)
        self.exporter = BatchCTEExporter(
            session_factory=async_sessionmaker(
                bind=create_async_engine(
                    url=db_url,
                    pool_size=db_pool_size,
                    max_overflow=db_max_overflow,
                    echo=db_sqlalchemy_echo,
                ),
                expire_on_commit=False,
            ),
            batch_size=db_import_batch_size,
            max_concurrent_batches=db_max_concurrent_batches,
        )
        self._requests_session: object = ...

    def run(self):
        """Entry point for starting the data transfer process.

        Initializes and runs the complete asynchronous pipeline from
        web scraping to database export.
        """
        asyncio.run(self._parse_files_to_website())

    @log
    async def _parse_files_to_website(self) -> None:
        """Main asynchronous workflow for processing schedule data.

        Coordinates the multi-step process: fetches website content,
        extracts Excel file links, downloads and parses files, and
        exports data to database with proper concurrency controls.
        """
        content: str = await web.get_website_content()
        doc: web.Document = web.Document(content=content)

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
        """Downloads Excel schedule file from the university website.

        :param end_url: URL suffix for the Excel file download
        :return: Parsed ExcelFile object or None if download fails
        :raises: aiohttp.ClientPayloadError on network issues
        """
        url: str = rf"https://www.sevsu.ru{end_url}"
        try:
            async with self._requests_session.get(url) as response:
                response.raise_for_status()
                bin_file: BytesIO = BytesIO(await response.read())
            return xls.ExcelFile(bin_file)
        except aiohttp.client_exceptions.ClientPayloadError as e:
            print(e)

    async def _handle_xls_file(
        self, tmp_data: Dict[str, Any], excel_file_url: str
    ) -> None:
        """Processes individual Excel file through the parsing pipeline.

        Downloads Excel file, extracts worksheets, and spawns processing
        tasks for each worksheet with rate limiting.

        :param tmp_data: Metadata about the Excel file
        :param excel_file_url: Download URL for the Excel file
        """
        async with self.requests_limit:
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
        """Processes individual worksheet and exports data to database.

        Extracts schedule data from worksheet rows and prepares structured
        data for batch export to the database.

        :param xls_sheet: Worksheet object containing schedule data
        :param tmp_data: Accumulated metadata for this worksheet
        """
        async for data in xls_sheet.run_data_stream():
            tmp_data.update(data)
            data = _create_data_for_export(tmp_data)
            await self.exporter.add(data)


@dataclass(frozen=True)
class DataFields:
    """Dataclass defining field names for data transfer operations.

    Provides consistent key names for accessing structured data throughout
    the transfer pipeline, bridging web, Excel, and database field names.
    """

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


def _create_data_for_export(tmp: Dict[str, Any]) -> Dict:
    """Transforms raw schedule data into structured export format.

    Converts combined web and Excel data into table-structured format
    suitable for database export, including hashcode generation for
    duplicate detection and dependency resolution.

    :param tmp: Combined data from web scraping and Excel parsing
    :return: Nested dictionary structured by table with metadata
    """
    data: Dict[str, Any] = {
        tables.Week.__tablename__: {
            "fields": {
                "year": tmp.get(xls.DataFields.week_year, ""),
                "title": tmp.get(xls.DataFields.week_title, ""),
                "start_date": tmp.get(xls.DataFields.week_start_date, ""),
                "end_date": tmp.get(xls.DataFields.week_end_date, ""),
                "semester": tmp.get(web.DataFields.inlist_title, ""),
            },
            "service_information": {
                "dependency": tables.Week.__dependencies__,
                "unique_keys": tables.Week.__unique_keys__,
                "hashcode": ...,
            },
        },
        tables.Group.__tablename__: {
            "fields": {
                "name": tmp.get(xls.DataFields.group, ""),
                "course": tmp.get(web.DataFields.filename, ""),
                "institute": tmp.get(web.DataFields.list_title, ""),
            },
            "service_information": {
                "dependency": tables.Group.__dependencies__,
                "unique_keys": tables.Group.__unique_keys__,
                "hashcode": ...,
            },
        },
        tables.Lesson.__tablename__: {
            "fields": {
                "study_form": tmp.get(web.DataFields.head, ""),
                "week_id": "",
                "group_id": "",
                "weekday": tmp.get(xls.DataFields.day, ""),
                "date": tmp.get(xls.DataFields.date, ""),
                "number": tmp.get(xls.DataFields.number, ""),
                "start_time": tmp.get(xls.DataFields.start_time, ""),
                "title": tmp.get(xls.DataFields.title, ""),
                "teacher": tmp.get(xls.DataFields.teacher, ""),
                "type_": tmp.get(xls.DataFields.type_, ""),
                "classroom": tmp.get(xls.DataFields.classroom, ""),
            },
            "service_information": {
                "dependency": {"week": ..., "group": ...},
                "unique_keys": tables.Lesson.__unique_keys__,
                "hashcode": ...,
            },
        },
    }

    group_hashcode: hash = hash(json.dumps(data[tables.Group.__tablename__]["fields"]))
    week_hashcode: hash = hash(json.dumps(data[tables.Week.__tablename__]["fields"]))
    lesson_hashcode: hash = hash(
        json.dumps(data[tables.Lesson.__tablename__]["fields"])
    )

    data[tables.Week.__tablename__]["service_information"]["hashcode"] = week_hashcode
    data[tables.Group.__tablename__]["service_information"]["hashcode"] = group_hashcode
    data[tables.Lesson.__tablename__]["service_information"][
        "hashcode"
    ] = lesson_hashcode

    data[tables.Lesson.__tablename__]["service_information"]["dependency"] = {
        "week": week_hashcode,
        "group": group_hashcode,
    }

    return data


if __name__ == "__main__":
    engine: GlobalDataTransfer = GlobalDataTransfer()
    engine.run()
