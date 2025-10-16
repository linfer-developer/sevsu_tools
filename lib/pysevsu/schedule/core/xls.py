# excel_processor.py

import asyncio
import openpyxl
from typing import Any
from typing import List
from typing import Dict
from typing import Any
from typing import Optional
from io import BytesIO


class ExcelFile:
    """The ExcelFile class provides tools for working with Excel a file from 
    the SevSU schedule website.
    It is used for processing Excel files with the university schedule.

    Attributes:
        file: IOBytes
            IOBytes object (Excel file).
        website_path: str
            The path to the Excel file on the website with the SevGU schedule.

    Methods:
        sheetnames()
            Returns a list of Excel file worksheets.
        async_import()
            Asynchronous import to the database. Creates Worksheet class 
            objects (a worker object an Excel sheet of a file) and collects 
            data from it.
        import_()
            Synchronous import to the database. Creates Worksheet class 
            objects (a worker object an Excel sheet of a file) and collects 
            data from it.

    """

    def __init__(self, file: BytesIO):
        self.file = openpyxl.load_workbook(
            filename=file, 
            read_only=True
        ) 

    @property
    def sheetnames(self) -> List[str]:
        """Returns a list of worksheet names."""

        return self.file.sheetnames

    async def async_generate_schedule_worksheets(self) -> object:
        """Asynchronous import to the database. Creates Worksheet objects and 
        collects data from them.

        It is commonly used in other asynchronous functions and methods. 

        """

        for sheetname in self.sheetnames:
            if sheetname.startswith("уч.н."):
                yield Worksheet(self.file[sheetname], sheetname)


class Worksheet:
    """Data collector from an Excel sheet.
 
    It is used as part of data collection from an Excel worksheet, where the 
    current class accepts a Worksheet object (an Excel file sheet) as a 
    required parameter. 

    It mainly provides tools for dynamically collecting data from an Excel 
    worksheet file implemented using generator methods.

    The class associated with it can be called :class`.dataflows`, which 
    provides the possibility of modification using callbacks.
    .. seealso::
        :class`.dataflows`

    """

    def __init__(
        self, 
        content: ExcelFile, 
        title: Optional[str] = ...
    ):
        """Construct a new :class:`.Handler`.

        :param data: raw data from the worksheet (cache). Strictly without 
         any preliminary changes, because the andler relies on the default cell
         layout.
        :type data: list (two-dimensional array)

        """

        self.content = content
        self.title = title
        self.data = self._load_cache()
        
        if self._max_row < 50:
            raise RuntimeError("Invalid size of the worksheet.")

    def _load_cache(self) -> List[List[str]]:
        """Caching data from an Excel file sheet."""

        return [
            [cell.value for cell in row] 
            for row in self.content.rows
        ]

    @property
    def _max_row(self) -> int:
        """Returns the number of rows (row length)."""
        
        return len(self.data)
    
    @property
    def _max_col(self) -> int:
        """Returns the number of rows (column length)."""

        return len(self.data[0])

    def _cell(self, row: int, column: int) -> Optional[str]:
        """Safe access to the cell."""

        try: 
            return self.data[row][column]
        except IndexError: 
            return None

    async def generate_lessons(self) -> object:
        """Generates lesson dictionaries.

        The method is used both for self-generation and in :class`.dataflows`
        with the possibility of extension using callbacks 
        :class`.auxiliary_processing_methods`.

        :yield: A lesson dictionary consisting of the following keys (weekday, 
         date, start time, number, title, type).
        :rtype: dict

        Example::
            async for data in Hander_obj.start_lessons_search():
                print(data)

        The result of executing this code, respectively, has the form:
        .. code-block:: text
            ...
            {'Время': '20:40', 'Занятие': 'Конституционное право, Коваленко Ник
            ита Алексеевич (VI-48)', 'Тип': 'ПЗ', 'Аудитория': 'II-2.7', 'Групп
            а': 'Ю/б-24-3-о', 'День': 'Суббота', 'Дата': '13.09.2025', '№заняти
            я': '8'}
            {'Время': '20:40', 'Занятие': 'подгр.:2(из 2),Иностранный язык, Пот
            овская Ксения Сергеевна', 'Тип': 'ПЗ', 'Аудитория': 'VI-48', 'Груп
            па': 'Ю/б-24-3-о', 'День': 'Суббота', 'Дата': '13.09.2025', '№заня
            тия': '8'}
            {'Время': '20:40', 'Занятие': 'Конституционное право, Коваленко Ник
            ита Алексеевич (VI-48)', 'Тип': 'ПЗ', 'Аудитория': 'II-2.7', 'Групп
            а': 'Ю/б-24-4-о', 'День': 'Суббота', 'Дата': '13.09.2025', '№занят
            ия': '8'}
            ...

        .. seealso::
            :class`.dataflows`

        """

        data: Dict[str, Any] = dict()

        for row in range(6, self._max_row):
            for col in range(3, self._max_col):
                group_col_value = self._cell(3, col)
                if group_col_value:
                    # example: "Группа : ИТ/б-24-2-о" -> "ИТ/б-24-2-о"
                    #          "Группа : ИТ/б-24-2-о" -> "Группа"
                    title = group_col_value.split()[0]
                    code = group_col_value.split()[-1]
                    data[title] = code

                col_title_value = self._cell(4, col) # example: "Дата"
                # Since the cells are combined, parts of the 
                # lesson attributes are on different rows.
                if (
                    not col_title_value or 
                    col_title_value.startswith("подгруппа")
                ):
                    col_title_value = self._cell(5, col) # example: "Занятие"

                value = self._cell(row, col)
                if value:
                    # example: {..., "Занятие" : "История России"}
                    #          {..., "Занятие" : "История России", Тип : "ПЗ"}
                    data[col_title_value] = str(value).strip()
                elif (
                    not value and 
                    col_title_value in (
                        "Занятие", 
                        "Тип", 
                        "Аудитория"
                    )
                ): 
                    try: del data[col_title_value]
                    except Exception: continue


                if (
                    col_title_value == "Аудитория" 
                    and len(data) >= 6
                    and "Занятие" in data.keys()
                ):
                    # After "Аудитория", the cell of another lesson is guaranteed 
                    # to begin. The length of the dictionary should be 6 since the 
                    # required keys are ("День", "Дата", "Время", "№занятия", 
                    # "Занятие")
                    yield data

    async def generate_groups(self) -> object:
        """Generates dictionaries of student groups.

        The method is used for asynchronous generation of student groups; 
        :class`.dataflows` extends the current functionality using callbacks 
        :class`.apm`.

        :yield: A student group dictionary consisting of a single name key 
         and its value.
        :rtype: dict

        Example::
            async for data in Hander_obj.start_group_search():
                print(data)

        The result of executing this code, respectively, has the form:
        .. code-block:: text
            ...
            {"name" : "ИТ/б-24-2-о"}
            {"name" : "ИТ/б-24-1-о"}
            {"name" : "ИБ/б-25-6-о"}
            ...

        .. seealso::
            :class`.dataflows`

        """

        for col in range(3, self.max_col):
            value = self._cell(3, col)
            if value: # example: "Группа : ИТ/б-24-2-о"
                yield {
                    'name' : value.split()[-1]
                }

    def get_dates_of_the_week(self) -> Dict[str, str]:
        """Returns the date range of the week, i.e. the start and end dates 
        of the student week. 

        The method is used to obtain additional data to expand existing 
        information about the student week. Used in :meth:`.methodname`.
        
        :return: a dictionary with two keys and values(the date of t
         he beginning of the week, the date of the end of the week).
        :rtype: dict

        The result of executing this code, respectively, has the form:
        .. code-block:: text
            {"start_date" : "21.04.2025", "end_date" : "27.04.2025"}

        .. seealso::
            :class`.dataflows`
        
        """

        return {
            "start_date" : self._cell(6, 1), 
            "end_date" : self._cell(46, 1)
        }
    

if __name__ == "__main__":
    asyncio.create_task()