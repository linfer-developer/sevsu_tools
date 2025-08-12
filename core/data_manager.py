import asyncio
from typing import Optional
from basedate.tables import *

@classmethod
async def import_(queue: asyncio.Queue):
    while True:
        db_item = await queue.get()
        if db_item == "STOP":
            break
        await asyncio.sleep(2) #TODO: имитация работы БД

class DataPreparer:

    def __init__(self, data: list):
        self.data = data 
        self.additional_data: dict = { 
            "week_number" : 0,
            "study_form" : "undefined", 
            "institute" : "undefined", 
            "semester" : "undefined", 
            "full_course_name" : "undefined" 
        }

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

        self.import_week_queue: asyncio.Queue = asyncio.Queue()
        self.import_group_queue: asyncio.Queue = asyncio.Queue()
        self.import_lesson_queue: asyncio.Queue = asyncio.Queue()

        self.import_week_task = asyncio.create_task(import_(self.import_week_queue))
        self.import_group_task = asyncio.create_task(import_(self.import_group_queue))
        self.import_lesson_task = asyncio.create_task(import_(self.import_lesson_queue))

    def _cell(self, row: int, col: int) -> Optional[str]:
        try: 
            return self.data[row][col]
        except IndexError: 
            return None

    async def async_import_data(self) -> Optional[list]:
        row: int = 3 

        week_obj: Week = Week(
            start_date=self._start_week_date,
            end_date=self._end_week_date,
            semester=self._semester,
            number=self._week
        ); await self.import_week_queue.put(week_obj)

        await asyncio.gather(*[
            self._import_group(
                row=row, 
                col=col, 
                week_obj=week_obj
            )
            for col in range(0, self._max_col)
                if self._cell(row, col)
            ], 
            self.import_week_task, 
            self.import_group_task, 
            self.import_lesson_task
        )

        self.import_week_queue.put("STOP")
        self.import_group_queue.put("STOP")
        self.import_lesson_queue.put("STOP")

    async def _import_group(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        week_obj: Optional[object] = None
    ) -> None:
        if self._cell(row, col):
            group: str = self._cell(row, col).split()[-1]
            group_obj: Group = Group(
                name=group, 
                course=self._course, 
                study_form=self._study_form, 
                institute=self._institute
            ); await self.import_group_queue.put(group_obj)

            await asyncio.gather(*[
                self._call_lesson_iteration(
                    row=row+3+8*number,
                    col=col,
                    week_obj=week_obj,
                    group_obj=group_obj
                ) 
                for number in range(0, 6)
            ]) 

    async def _call_lesson_iteration(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        week_obj: Optional[object] = None, 
        group_obj: Optional[object] = None
    ) -> None:
        weekday: str = self._cell(row, col) 
        date: str = self._cell(row, col+1) 

        await asyncio.gather(*[ 
            self._call_import_lesson(
                row=row+offset, 
                col=col+2,
                import_data={
                    "день недели" : weekday, 
                    "дата" : date
                },
                week_obj=week_obj, 
                group_obj=group_obj
            ) 
            for offset in range(0, 8)
        ]) 

    async def _call_import_lesson(
        self, 
        row: Optional[str] = None, 
        col: Optional[str] = None,
        import_data: Optional[dict] = None,
        week_obj: Optional[object] = None, 
        group_obj: Optional[object] = None
    ) -> None:
        tasks: list = []

        lesson_number: int = self._cell(row, col) 
        lesson_start_time: str = self._cell(row, col+1) 
        lesson_name: Optional[str] = None 
        lesson_type: Optional[str] = None 
        lesson_classroom: Optional[str] = None 

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
                    import_data.update({
                        "номер" : lesson_number, 
                        "начало" : lesson_start_time
                    })

                    task = asyncio.create_task(self._import_lesson(
                        lesson_name=lesson_name, 
                        lesson_type=lesson_type, 
                        lesson_classroom=lesson_classroom, 
                        import_data=import_data, 
                        week_obj=week_obj, 
                        group_obj=group_obj
                    ))
                    tasks.append(task)

        await asyncio.gather(*tasks)

    async def _import_lesson(
        self, 
        lesson_name: str,
        lesson_type: Optional[str] = None,
        lesson_classroom: Optional[str] = None,
        import_data: Optional[str] = None,
        week_obj: Optional[str] = None,
        group_obj: Optional[str] = None
    ) -> None:
        if not(lesson_classroom and lesson_type):
            import_data.update({
                "название" : "".join(lesson_name.split(", ")[0:-1]),
                "преподаватель" : lesson_name.split(", ")[-1],
                "тип" : lesson_type,
                "аудитория" : lesson_classroom
            })

            try:
                lesson_obj: Lesson = Lesson(
                    group_id=group_obj.id,
                    week_id=week_obj.id,
                    weekday=import_data["день недели"],
                    date=import_data["дата"],
                    number=import_data["номер"],
                    start_time=import_data["начало"],
                    title=import_data["название"],
                    teacher=import_data["преподаватель"],
                    type_=import_data["тип"],
                    classroom=import_data["аудитория"]
                ); await self.import_lesson_queue.put(lesson_obj)
                return

            except Exception as e:
                print(f"Warning: exception in line 286 of the "
                      "_import_lesson method of the " \
                      f"data_manager module. Exception: {e}")
                return

        if lesson_classroom and lesson_type:
            lesson_name = lesson_name.splitlines()
            lesson_type = lesson_type.splitlines()
            lesson_classroom = lesson_classroom.splitlines()

            for offset in range(0, len(lesson_name)):
                import_data.update({
                    "название" : "".join(lesson_name[offset].split(", ")[0:-1]),
                    "преподаватель" : lesson_name[offset].split(", ")[-1],
                    "тип" : lesson_type[offset],
                    "аудитория" : lesson_classroom[offset]
                })

                try:
                    lesson_obj: Lesson = Lesson(
                        group_id=group_obj.id,
                        week_id=week_obj.id,
                        weekday=import_data["день недели"],
                        date=import_data["дата"],
                        number=import_data["номер"],
                        start_time=import_data["начало"],
                        title=import_data["название"],
                        teacher=import_data["преподаватель"],
                        type_=import_data["тип"],
                        classroom=import_data["аудитория"]
                    ); await self.import_lesson_queue.put(lesson_obj)

                except Exception as e:
                    print(f"Warning: exception in line 286 "
                          "of the _import_lesson method of " 
                          f"the data_manager module. Exception: {e}")
                    continue