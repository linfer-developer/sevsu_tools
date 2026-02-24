"""excel.py

The excel module provides tools for parsing schedule Excel files from
Sevastopol State University. It extracts structured data from worksheets
containing academic schedules.

The module is designed to work within the core package and should not be
used directly outside of it. For external usage, prefer the API layer.

TODO: Standardize documentation format.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import openpyxl

from src.pysevsu.exceptions import InvalidSheetSizeError


@dataclass(frozen=True)
class FileConfig:
    """Configuration dataclass for Excel file processing settings.

    :param sheetname_prefix: Prefix for identifying relevant worksheets
                             in the Excel file.
    """

    sheetname_prefix: str = "уч.н."


@dataclass(frozen=True)
class WorksheetConfig:
    """Configuration dataclass defining worksheet structure and layout.

    Contains cell positions, row ranges, and column identifiers for
    extracting schedule data from Excel worksheets.

    :param min_worksheet_size: Minimum number of rows for a valid worksheet
    :param header_row: Row index containing column headers (0-based)
    :param subgroup_row: Row index containing subgroup information
    :param date_start_row: Starting row index for date data
    :param date_end_row: Ending row index for date data
    :param name_cell_lesson_day: Column title for lesson day
    :param name_cell_lesson_date: Column title for lesson date
    :param name_cell_lesson_number: Column title for lesson number
    :param name_cell_lesson_start_time: Column title for lesson start time
    :param name_cell_lesson_title: Column title for lesson title
    :param name_cell_lesson_type: Column title for lesson type
    :param name_cell_lesson_classroom: Column title for classroom
    """

    min_worksheet_size: int = 14
    header_row: int = 4
    subgroup_row: int = 5
    date_start_row: int = 6
    date_end_row: int = 46
    title_change_point = "подгруппа"
    name_cell_lesson_day: str = "День"
    name_cell_lesson_date: str = "Дата"
    name_cell_lesson_number: str = "№занятия"
    name_cell_lesson_start_time: str = "Время"
    name_cell_lesson_title: str = "Занятие"
    name_cell_lesson_type: str = "Тип"
    name_cell_lesson_classroom: str = "Аудитория"


@dataclass(frozen=True)
class DataFields:
    """Dataclass defining FIELD names for structured data output.

    Used as keys in the resulting dictionaries to maintain consistency
    across the data pipeline.
    """

    week_year: str = "year"
    week_title: str = "title"
    week_start_date: str = "start_date"
    week_end_date: str = "end_date"
    group: str = "group"
    day: str = "lesson_day"
    date: str = "lesson_date"
    number: str = "lesson_number"
    start_time: str = "lesson_start_time"
    title: str = "lesson_title"
    teacher: str = "lesson_teacher"
    type_: str = "lesson_type"
    classroom: str = "lesson_classroom"


FIELD = DataFields
FILE_CONF = FileConfig
SHEET_CONF = WorksheetConfig


class ExcelFile:
    """Main class for handling Excel schedule files.

    Loads and processes Excel files containing academic schedules,
    providing access to relevant worksheets through a generator.

    :param file: File object or file path to load. Supported formats
                 are those compatible with openpyxl.
    """

    def __init__(self, file: object) -> None:
        self.file = openpyxl.load_workbook(filename=file, read_only=True)

    async def run_worksheets_stream(self) -> object:
        """Asynchronous generator yielding relevant worksheets from the file.

        Iterates through all worksheets and yields only those whose names
        start with the prefix defined in FileConfig.

        :yield: Worksheet objects containing schedule data
        """
        for sheetname in self.file.sheetnames:
            if sheetname.startswith(FILE_CONF.sheetname_prefix):
                yield Worksheet(self.file[sheetname])


class Worksheet:
    """Represents a single worksheet containing schedule data.

    Handles data extraction, validation, and transformation from Excel
    worksheet format to structured dictionaries.

    :param content: openpyxl Worksheet object containing the raw data
    """

    def __init__(self, content: openpyxl.worksheet.worksheet.Worksheet) -> None:
        self.content = content
        self.title = content.title
        self.data = self.load_cache()

        if self.max_rows < SHEET_CONF.min_worksheet_size:
            raise InvalidSheetSizeError(
                actual_size=self.max_rows,
                expected_min=SHEET_CONF.min_worksheet_size,
                sheet_name=self.title,
            )

    def load_cache(self) -> List[List[Any]]:
        """Loads and caches worksheet data into memory.

        Converts the worksheet into a 2D list for faster access during
        processing. This is particularly useful for read-only operations.

        :return: 2D list representation of worksheet data
        """
        return [
            [cell.value for cell in row] 
            for row in self.content.rows
        ]

    @property
    def max_rows(self) -> int:
        """Returns the total number of rows in the worksheet."""
        return len(self.data)

    @property
    def max_columns(self) -> int:
        """Returns the total number of columns in the worksheet."""
        return len(self.data[0])

    def _cell(self, row: int, column: int) -> Optional[str]:
        """Safely retrieves cell value with bounds checking.

        :param row: Row index (0-based)
        :param column: Column index (0-based)
        """
        try:
            return self.data[row][column]
        except IndexError:
            return None

    def _get_column_title(self, column: int) -> str:
        """Determines the appropriate column title for data extraction.

        Checks both header and subgroup rows to find the correct title
        for a given column.

        :param column: Column index to check
        """
        title = self._cell(SHEET_CONF.header_row, column)
        if not title or title.startswith(SHEET_CONF.title_change_point):
            title = self._cell(SHEET_CONF.subgroup_row, column)
        return title

    def _reset(self, tmp: Dict[str, Any]) -> None:
        """Resets temporary dictionary fields for type and classroom.

        Clears the lesson type and classroom fields in preparation for
        processing new data.

        :param tmp: Temporary dictionary to reset
        """
        tmp.update({FIELD.type_: "", FIELD.classroom: ""})

    async def run_data_stream(self) -> object:
        """Asynchronous generator extracting structured data from worksheet.

        Processes each cell in the worksheet, extracting relevant schedule
        information and yielding complete lesson records. Handles complex
        cell structures like multi-line entries.

        :yield: Dictionary containing structured lesson data
        """
        wdtm = _WorksheetDataTransferMethods
        translation = wdtm.translation()
        tmp: Dict[str, Any] = {}

        for row in range(self.max_rows):
            for column in range(self.max_columns):
                group_title_cell = self._cell(3, column)

                if group_title_cell:
                    group = group_title_cell.split(" : ")[-1].strip()
                    tmp[FIELD.group] = group

                tmp_title: str = self._get_column_title(column)
                value: str = self._cell(row, column)

                if value and tmp_title != value:
                    title: str = translation.get(tmp_title)
                    tmp[title] = value

                    if title == FIELD.title:
                        self._reset(tmp)
                    elif title == FIELD.classroom:
                        data = tmp.copy()

                        async for result in wdtm.run_cell_processing(data):
                            yield result

    def get_week_info(self) -> Dict[str, str]:
        """Extracts metadata about the academic week from the worksheet.

        Retrieves week title and date range from predefined worksheet
        positions.

        :return: Dictionary containing week metadata
        """
        return {
            FIELD.week_title: self.title,
            FIELD.week_start_date: self._cell(SHEET_CONF.date_start_row, 1),
            FIELD.week_end_date: self._cell(SHEET_CONF.date_end_row, 1),
        }


class _WorksheetDataTransferMethods:
    """Internal utility class for data transformation methods.

    Contains static methods for processing and transforming raw cell data
    into structured format. Not intended for direct external use.
    """

    @staticmethod
    def translation() -> Dict[str, str]:
        """Maps worksheet column titles to standardized FIELD names.

        :return: Dictionary mapping original titles to FIELD keys
        """
        return {
            SHEET_CONF.name_cell_lesson_day: FIELD.day,
            SHEET_CONF.name_cell_lesson_date: FIELD.date,
            SHEET_CONF.name_cell_lesson_number: FIELD.number,
            SHEET_CONF.name_cell_lesson_start_time: FIELD.start_time,
            SHEET_CONF.name_cell_lesson_title: FIELD.title,
            SHEET_CONF.name_cell_lesson_type: FIELD.type_,
            SHEET_CONF.name_cell_lesson_classroom: FIELD.classroom,
        }

    @staticmethod
    async def run_cell_processing(data: Dict[str, Any]) -> object:
        """Processes complex cell data containing multiple lessons.

        Handles cells with multi-line content, splitting and distributing
        data across multiple lesson records.

        :param tmp: Temporary dictionary with initial cell data
        :yield: Processed lesson dictionaries
        """
        wdtm = _WorksheetDataTransferMethods
        lesson_titles: List[str] = wdtm.split_cell_value(data[FIELD.title])
        lesson_types: List[str] = wdtm.split_cell_value(data[FIELD.type_])
        lesson_classrooms: List[str] = wdtm.split_cell_value(data[FIELD.classroom])
        iteration_length: int = len(lesson_titles)

        for index in range(iteration_length):
            full_title = lesson_titles[index]
            title, teacher = wdtm.parse_lesson_line(full_title)
            data.update({FIELD.title: title, FIELD.teacher: teacher})

            wdtm.process_string(
                iteration_length=iteration_length,
                list_=lesson_types,
                tmp=data,
                tmp_key=FIELD.type_,
                iteration_index=index,
            )
            wdtm.process_string(
                iteration_length=iteration_length,
                list_=lesson_classrooms,
                tmp=data,
                tmp_key=FIELD.classroom,
                iteration_index=index,
            )

            yield data

    @staticmethod
    def parse_lesson_line(str_: str) -> tuple:
        """Parses lesson title string to separate title from teacher.

        :param str_: Raw lesson title string
        :return: Tuple of (lesson_title, teacher_name)
        """
        if ", " in str_:
            tmp = str_.split(", ")
            title = " ".join(tmp[:-1])
            teacher = tmp[-1]
        else:
            title = str_
            teacher = ""

        return title, teacher

    @staticmethod
    def process_string(
        iteration_length: int,
        list_: List[str],
        tmp: Dict[str, str],
        tmp_key: str,
        iteration_index: int,
    ) -> None:
        """Processes and distributes multi-line string data.

        Ensures proper alignment of split data across multiple lesson
        records, handling cases where line counts don't match.

        :param iteration_length: Expected number of items
        :param list_: List of split string items
        :param tmp: Target dictionary to update
        :param tmp_key: Dictionary key to update
        :param iteration_index: Current iteration index
        """

        if iteration_length == len(list_):
            tmp[tmp_key] = list_[iteration_index]
            return

        tmp[tmp_key] = "".join(list_)

    @staticmethod
    def split_cell_value(str_: str) -> List[str]:
        return str_.strip().splitlines()


async def test(url):
    """Test function demonstrating module usage with remote Excel file.

    Downloads and processes an Excel schedule file from a URL, printing
    extracted data. Used for development and debugging purposes.

    :param url: URL to download Excel file from
    """
    from io import BytesIO

    import aiohttp

    async with aiohttp.ClientSession() as s:
        async with s.get(url) as response:
            response.raise_for_status()
            xls_content: BytesIO = BytesIO(await response.read())

    xls = ExcelFile(xls_content)

    async for sheet in xls.run_worksheets_stream():
        print(sheet)
        async for i in sheet.run_data_stream():
            print(i)


if __name__ == "__main__":
    URL = "https://www.sevsu.ru/univers/shedule/download.php?file=sZxsuB9JgV170U3cLSLLpg%3D%3D"
    asyncio.run(test(URL))
