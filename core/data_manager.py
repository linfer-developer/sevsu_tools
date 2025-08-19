import asyncio
from typing import Optional

from database.tables import *
from database.interface import *
from core.queue_manager import ImportQueues

from copy import deepcopy as copy


class DataPreparer:

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
                "Количество рядов рабочего листа " \
                "Excel файла не может быть меньше 50."
            )

    def _cell(self, row: int, col: int) -> Optional[str]:
        try: 
            return self.data[row][col]
        except IndexError: 
            return None

    async def async_import_data(self) -> Optional[list]:
        row: int = 3 
        data: dict = {
            "week" : {
                "start_date" : self._start_week_date,
                "end_date" : self._end_week_date,
                "semester" : self._semester,
                "number" : self._week
            }
        }
        await ImportQueues.put_week(data)

        tasks: list = []
        for col in range(0, self._max_col):
            if self._cell(row, col):
                task = asyncio.create_task(
                    self._import_group(
                        row=row, 
                        col=col, 
                        data=copy(data)
                    )
                ); tasks.append(task)
        await asyncio.gather(*tasks)

    async def _import_group(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None
    ) -> None:
        if self._cell(row, col):
            group: str = self._cell(row, col).split()[-1]
            data.update({
                'group' : {
                    'name': group, 
                    'course': self._course, 
                    'study_form': self._study_form, 
                    'institute': self._institute
                }
            })
            await ImportQueues.put_group(data)

            tasks: list = []
            for offset in range(0, 6):
                task = asyncio.create_task(
                    self._call_lesson_iteration(
                        row=row+3+8*offset,
                        col=col,
                        data=copy(data)
                    )
                ); tasks.append(task)
            await asyncio.gather(*tasks)

    async def _call_lesson_iteration(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None
    ) -> None:
        weekday: str = self._cell(row, col) 
        date: str = self._cell(row, col+1) 
        data.update({
            'lesson' : {
                "weekday" : weekday, 
                "date" : date
            }
        })

        tasks: list = []
        for offset in range(0, 8):
            task = asyncio.create_task(
                self._call_import_lesson(
                    row=row+offset, 
                    col=col+2,
                    data=copy(data),
                )
            ); tasks.append(task)
        await asyncio.gather(*tasks)

    async def _call_import_lesson(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        lesson_number: int = self._cell(row, col) 
        lesson_start_time: str = self._cell(row, col+1) 
        lesson_name: Optional[str] = None 
        lesson_type: Optional[str] = None 
        lesson_classroom: Optional[str] = None 

        tasks: list = []
        for offset in [0, 3, 6]:
            if not self._cell(5, col+offset+2):
                continue

            col_title: str = self._cell(5, col+offset+2).lower().strip() 
            if col_title in ["занятие", "тип", "аудитория"]:

                if self._cell(row, col+offset+2):
                    lesson_name = self._cell(row, col+offset+2).strip()

                if self._cell(row, col+offset+3):
                    lesson_type = self._cell(row, col+offset+3).strip()
                    lesson_classroom = self._cell(row, col+offset+4).strip()

                if lesson_name and lesson_type:      
                    data['lesson'].update({
                        "number" : lesson_number, 
                        "start_time" : lesson_start_time
                    })

                    task = asyncio.create_task(self._import_lesson(
                        lesson_name=lesson_name, 
                        lesson_type=lesson_type, 
                        lesson_classroom=lesson_classroom, 
                        data=copy(data), 
                    ))
                    tasks.append(task)

        await asyncio.gather(*tasks)

    async def _import_lesson(
        self, 
        lesson_name: str,
        lesson_type: Optional[str] = None,
        lesson_classroom: Optional[str] = None,
        data: Optional[str] = None
    ) -> None:
        if not(lesson_classroom and lesson_type):
            data['lesson'].update({
                "title" : "".join(lesson_name.split(", ")[0:-1]),
                "teacher" : lesson_name.split(", ")[-1],
                "type" : lesson_type,
                "classroom" : lesson_classroom
            })
            
            await ImportQueues.put_lesson(data)

        if lesson_classroom and lesson_type:
            lesson_name = lesson_name.splitlines()
            lesson_type = lesson_type.splitlines()
            lesson_classroom = lesson_classroom.splitlines()

            for offset in range(0, len(lesson_name)):
                data['lesson'].update({
                    "title" : "".join(lesson_name[offset].split(", ")[0:-1]),
                    "teacher" : lesson_name[offset].split(", ")[-1],
                    "type" : lesson_type[offset],
                    "classroom" : lesson_classroom[offset]
                })
                await ImportQueues.put_lesson(data)
