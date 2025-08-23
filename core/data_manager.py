import asyncio
from typing import Optional
from copy import deepcopy as copy
from database.tables import *
from database.interface import *
from core.queue_manager import ImportQueues

LESSON_OFFSETS: list = [0, 3, 6]
COLS_TITLES = ["занятие", "тип", "аудитория"]

class WorksheetCacheHandler:

    def __init__(
        self, 
        data: list,
        additional_data: dict = { 
            "week_number" : 0,
            "study_form" : "undefined", 
            "institute" : "undefined", 
            "semester" : "undefined", 
            "full_course_name" : "undefined" 
        }
    ):
        self.data = data 
        self.additional_data = additional_data

        try:
            self._week: int = self.additional_data["week_number"] 
            self._start_week_date = self._cell(6, 1)
            self._end_week_date = self._cell(46, 1)
            self._course: int = self.additional_data["full_course_name"] 
            self._study_form: str = self.additional_data["study_form"] 
            self._institute: str = self.additional_data["institute"] 
            self._semester: str = self.additional_data["semester"]

        except Exception as Error:
            raise RuntimeError("Ошибка при инициализации вспомогательных " \
                              f"данных, неверный формат словаря: {Error}")

        self._max_row: int = len(self.data)
        self._max_col: int = len(self.data[0])

        if self._max_row < 50:
            raise RuntimeError(
                "Количество рядов рабочего листа Excel " \
                "файла не может быть меньше 50."
            )

    def _cell(self, row: int, col: int) -> Optional[str]:
        try: 
            return self.data[row][col]
        except IndexError: 
            return None

    async def async_import_data(self) -> Optional[list]:
        data = dict()
        await self._import_week(data=data)
        await self._process_group_cells(data=data)

    async def _import_week(self, data: dict):
        data.update({
            "week" : {
                "start_date" : self._start_week_date,
                "end_date" : self._end_week_date,
                "semester" : self._semester,
                "number" : self._week
            }
        })
        print(data)
        await ImportQueues.put_week(data)

    async def _process_group_cells(
        self, 
        data: Optional[dict] = None
    ) -> None:
        row: int = 3 # Group headings. Example: "Группа : ИТ/б-24-1-о"
        tasks = list()
        for col in range(0, self._max_col):
            if self._cell(row, col):
                # Preparing data inside the method
                tasks.append(asyncio.create_task(self._import_group(
                    row=row, col=col, data=data
                )))
                tasks.append(asyncio.create_task(
                    self._process_student_day_cells(
                        row=row+3, 
                        col=col, 
                        data=copy(data)
                    )
                ))
        await asyncio.gather(*tasks)

    async def _import_group(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None
    ) -> None:
        # var group. Example: "ИТ/б-24-1-о"
        group: str = self._cell(row, col).split()[-1] 
        data.update({
            'group' : {
                'name': group, 
                'course': self._course, 
                'study_form': self._study_form, 
                'institute': self._institute
            }
        })
        print(data)
        # Note: indirect import via queue
        await ImportQueues.put_group(data)

    async def _process_student_day_cells(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None
    ) -> None:
        tasks = list()
        # column title: "День"
        # offset: 6 student days in the table
        for offset in range(0, 6):
            # row: indent 3 cells down to get to the first day of the week. 
            # Next, 8 pairs (the maximum number per day) multiplied by the 
            # current day of the iteration, where the first day is 0.
            row = row+8*offset # example: "Понедельник".

            data = self._create_lesson_data(row=row, col=col, data=data)
            tasks.append(asyncio.create_task(
                self._process_full_lesson_cells(
                    row=row,
                    col=col,
                    data=copy(data)
                )
            ))

        await asyncio.gather(*tasks)
    
    def _create_lesson_data(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None
    ) -> dict:
        """Creates a part of the data in the dictionary (day of the week 
        and date) for later use in importing the lesson.
        """
        weekday: str = self._cell(row, col) 
        date: str = self._cell(row, col+1) 
        data.update({'lesson' : {"weekday" : weekday, "date" : date}})
        print(data)
        return data

    async def _process_full_lesson_cells(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None
    ):
        tasks: list = []
        # offset: 8 lessons in the table
        for offset in range(0, 8):
            row = row+offset # example: 1
            col = col+2 # title: "№занятия"
            tasks.append(asyncio.create_task(
                self._process_lesson_cell_data(
                    row=row, 
                    col=col,
                    data=copy(data),
                )
            ))
        await asyncio.gather(*tasks)

    async def _process_lesson_cell_data(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        number: int = self._cell(row, col) 
        start_time: str = self._cell(row, col+1) 
        full_title: Optional[str] = None 
        type_: Optional[str] = None 
        classroom: Optional[str] = None 

        tasks: list = list()
        # offset: 
        for offset in LESSON_OFFSETS:
            row_of_title = 5
            col = col+2+offset
            if not self._cell(row_of_title, col):
                break

            col_title: str = self._cell(row_of_title, col).lower().strip() 
            if col_title in COLS_TITLES:
                col_title_lesson = self._cell(row, col)
                col_title_type = self._cell(row, col+1)
                col_title_classroom = self._cell(row, col+2)

                if col_title_lesson:
                    full_title = col_title_lesson.strip()

                if col_title_type:
                    type_ = col_title_type.strip()
                    if col_title_classroom:
                        classroom = col_title_classroom.strip()

                if full_title and type_:
                    data['lesson'].update({
                        "number" : number, 
                        "start_time" : start_time
                    })
                    print(data)
                    task = asyncio.create_task(self._import_lesson(
                        title=full_title, 
                        type_=type_, 
                        classroom=classroom, 
                        data=copy(data), 
                    ))
                    tasks.append(task)

        await asyncio.gather(*tasks)

    async def _import_lesson(
        self, 
        title: str,
        type_: Optional[str] = None,
        classroom: Optional[str] = None,
        data: Optional[str] = None
    ) -> None:
        if not(classroom and type_):
            data['lesson'].update({
                "title" : "".join(title.split(", ")[0:-1]),
                "teacher" : title.split(", ")[-1],
                "type" : type_,
                "classroom" : classroom
            })
            print(data)
            # asyncio.create_task(ImportQueues.put_lesson(data))

        if classroom and type_:
            title = title.splitlines()
            type_ = type_.splitlines()
            classroom = classroom.splitlines()

            for offset in range(0, len(title)):
                data['lesson'].update({
                    "title" : "".join(title[offset].split(", ")[0:-1]),
                    "teacher" : title[offset].split(", ")[-1],
                    "type" : type_[offset],
                    "classroom" : classroom[offset]
                })
                print(data)
                # asyncio.create_task(ImportQueues.put_lesson(data))
