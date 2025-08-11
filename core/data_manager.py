"""Обозначения, которые используются внутри модуля:
1) -> Зависимость в иеерархии при которой метод является вызываемым в следующем методе.
2) >>> Зависимость при которой метод, который вызывается создает логическую иерархию, но 
при этом вызывающий метод логически несвязный со следующей иерархией. Обычно это присуще 
глобальным методам сбора данных, иные случаи - плохая практика. 
3) () Отделение разных логических иерархий.
"""
import asyncio

from typing import Optional
from basedate.interface import DatabaseInterface as dbi


class Importer:

    """Класс ExcelFile предоставляет инструменты для обработки Excel листа. 
    Работает в композиции с классом ExcelFile, объект этого класса является обязательным
    аттритубом WorksheetHandler.

    Секция - отдельный блок, выделяемый по смыслу, визуалу и логике для отдельной 
    реализации какой-либо функции. В секциях происходит локальный перебор данных 
    в зависимости от ее названия (группы, дня и пар).
    Выделяется три вида секций:
        1. Секция группы: включает в себя секции дня и секции пар. Представляет собой разбиения 
        таблицы по колонкам групп.
        2. Секция дня: включает в себя секции пар. Она представляет собой разбиение таблицы 
        секции группы по рядам. 
        3. Секция пар: является финальной секцией. Находится в секции дня и разбивает ее по рядам.
    Иерархия секций представлена ниже:
        Секция группы -> Секция дня -> Секция пар.
    """
    def __init__(self, data: list):
        self.data = data # Инициализированный ExcelFile
        self.additional_data: dict = { # Дополнительные данные к импорту
            "week_number" : 0, # Номер Excel листа
            "study_form" : "undefined", # Студентческая форма
            "institute" : "undefined", # Институт
            "semester" : "undefined", # Семестр
            "full_course_name" : "undefined" # Полное имя курса (1 курс бакалавриат)
        }
        self.max_row: int = len(self.data)
        self.max_column: int = len(self.data[0])

        if self.max_row < 50:
            raise RuntimeError("Количество рядов рабочего листа Excel файла не может быть меньше 50.")

        try:
            self.week: int = self.additional_data["week_number"] # Номер листа
            self.start_week_date = self.cell(6, 1)
            self.end_week_date = self.cell(46, 1)
            self.course: int = self.additional_data["full_course_name"] # Номер курса
            self.study_form: str = self.additional_data["study_form"] # Студенческая формы
            self.institute: str = self.additional_data["institute"] # Институт
            self.semester: str = self.additional_data["semester"] # Семестр
        except Exception as Error:
            raise RuntimeError("Ошибка при инициализации вспомогательных данных, неверный "
                              f"формат словаря: {Error}")

        MAX_CONCURRENT_SESSIONS = 10
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_SESSIONS)

    def cell(self, row: int, col: int) -> Optional[str]:
        """Безопасно возвращает значение ячейки."""
        try: return self.data[row][col]
        except IndexError: return

    async def async_import_data(self) -> Optional[list]:
        """Главный асинхронный метод для сбора данных с таблицы. 
        
        От этого методы следует иерархия: 
            x_iteration >>> (_import_group -> _call_lesson_iteration -> _call_import_lesson)
        """
        row: int = 3 # С этого ряда начинается строка с группами

        week_db: object = None # await dbi.create_week(
            # start_date=self.start_week_date,
            # end_date=self.end_week_date,
            # semester=self.semester,
            # number=self.week)

        await asyncio.gather(*[
            self._import_group(
                row=row, column=column, 
                week_db=week_db)
            for column in range(0, self.max_column)
                if self.cell(row, column)])

    async def _import_group(self, row: str, column: str,
                            week_db: Optional[object] = None):
        """Метод для сбора данных с секции группы. 

        От метода следует иерархия: 
            _import_group -> _call_lesson_iteration -> _call_import_lesson

        Реализация метода состоит в проверке на пустоты ячеек и сравнение с переданным
        фильтрами (если такие переданы). С помощью генератора в переменной study_days
        собирается список словарей дней, если те существуют (т.е. если есть пары в дне)
        
        :row str: ряд в Excel или первый индекс двумерном массиве в self.data
        :column str: столбце в Excel или второй индекс в двумерном массиве self.data
        """
        if self.cell(row, column):
            group: str = self.cell(row, column).split()[-1]

            group_db: object = None # await dbi.create_group(
                    # name=group, 
                    # course=self.course, 
                    # study_form=self.study_form, 
                    # institute=self.institute)

            await asyncio.gather(*[
                self._call_lesson_iteration(
                    # row+3+8*number: отступ 3 ряда вниз до дня недели.
                    # 8 - максимальное количество пар. 
                    row=row+3+8*number, # Ячейка начала дня (день недели)
                    column=column,
                    week_db=week_db,
                    group_db=group_db) 
                # number - текущий день недели от 0. 
                # Шаг 6 дней, т.к. в неделе 6 учебных дней максимум
                for number in range(0, 6)]) 

    async def _call_lesson_iteration(self, row: str, column: str,
                                    week_db: Optional[object] = None, 
                                    group_db: Optional[object] = None):
        """Метод для сбора данных с секции дня.

        От метода следует иерархия: 
            _call_lesson_iteration -> _call_import_lesson
        
        Реализация метода состоит в сборе данных с ячеек названия дня, а затем в генерации 
        пар в текущем дне. В метод _call_import_lesson в генерации передается текущий ряд и колонка.

        :row str: ряд в Excel или первый индекс двумерном массиве в self.data
        :column str: столбце в Excel или второй индекс в двумерном массиве self.data
        """
        weekday: str = self.cell(row, column) # День недели
        date: str = self.cell(row, column+1) # Дата дня

        await asyncio.gather(*[ # Список словарей секций пар
            self._call_import_lesson(
                row=row+offset, column=column+2,
                export_lesson_data={"день недели" : weekday, 
                                    "дата" : date},
                week_db=week_db, 
                group_db=group_db
            ) # Ячейка начала пары (номер пары)
            for offset in range(0, 8)]) # Шаг 8 пар, т.к. в день 8 пар максимум
                # row+offset - к ряду приповляется шаг
                # column+2 - от дня недели 2 ячейки до номера пары

    async def _call_import_lesson(
        self, row: str, column: str,
        export_lesson_data: Optional[dict] = None,
        week_db: Optional[object] = None, 
        group_db: Optional[object] = None
    ) -> Optional[list]:
        """Метод для сбора данных с секции пары. Вызывается в методе _call_lesson_iteration 
        и является часть цикла (перебора данных).

        Релизация с выносом переменных lesson_name, lesson_type, lesson_classroom 
        за пределы цикла объясняется наличием двух типов расположения пар в расписании: 
        отдельные пары для подгрупп группы и общие пары для всей группы. В Excel файле 
        ячейки объедены, но openpyxl их не видит их как единое целую: для него не существуют 
        понятия объединенных ячеек при сборе данных, значение каждой объединенной ячейки 
        хранится в начальной ячейки от объедененных.

            Например: Физика: Электричество и магнетизм, Довгаленко Владимир Васильевич 
            (VII-403) существует для всей группы и объединена в 4 ячейках. 
            Тогда название существует только в 1 ячейке, а тип и аудитория после 4 ячейки.

        Собственно из примера видно, что в таком случае тип и аудитория не следуют 
        за 1 ячейкой, а только после 4.

        :row str: ряд в Excel или первый индекс двумерном массиве в self.data
        :column str: столбце в Excel или второй индекс в двумерном массиве self.data
        """
        tasks: list = []

        lesson_number: int = self.cell(row, column) # Номер пары
        lesson_start_time: str = self.cell(row, column+1) # Номер пары
        lesson_name: Optional[str] = None # Название пары
        lesson_type: Optional[str] = None # Тип пары
        lesson_classroom: Optional[str] = None # Аудитория пары

        # Начало цикла с шагом 3 ячейки, именно столько составляет расстояние между разными парами.
        for count in [0, 3, 6]:
            # Если ячейка названия колонки пуста, то переходим к следующей итерации цикла.
            if not self.cell(5, column+count+2):
                continue

            column_title: str = self.cell(5, column+count+2).lower().strip() # Ячейка названия колонки
            if column_title in ["занятие", "тип", "аудитория"]:

                # Если ячейка название не пуста, то значение непустой ячейки записывается в переменную
                if self.cell(row, column+count+2):
                    lesson_name = self.cell(row, column+count+2).strip()

                # Если ячейки типа и аудитории не пусты, то значение непустой ячейки записывается в переменную
                if self.cell(row, column+count+3): # проверка только по ячейке типа, ибо тогда и другая существует также
                    lesson_type = self.cell(row, column+count+3).strip()
                    lesson_classroom = self.cell(row, column+count+4).strip()

                if lesson_name and lesson_type:            
                    new_export_data: dict = {"номер" : lesson_number, "начало" : lesson_start_time}
                    export_lesson_data.update(new_export_data)
                    task = asyncio.create_task(self._import_lesson(
                        lesson_name=lesson_name, 
                        lesson_type=lesson_type, 
                        lesson_classroom=lesson_classroom, 
                        export_lesson_data=export_lesson_data, 
                        week_db=week_db, 
                        group_db=group_db))
                    tasks.append(task)

        await asyncio.gather(*tasks)

    async def _import_lesson(
        self, lesson_name: str,
        lesson_type: Optional[str] = None,
        lesson_classroom: Optional[str] = None,
        export_lesson_data: Optional[str] = None,
        week_db: Optional[str] = None,
        group_db: Optional[str] = None
    ) -> None:
        if not(lesson_classroom and lesson_type):
            try:
                lesson: dict = {"название" : "".join(lesson_name.split(", ")[0:-1]),
                                "преподаватель" : lesson_name.split(", ")[-1],
                                "тип" : lesson_type,
                                "аудитория" : lesson_classroom}
                export_lesson_data.update(lesson)
                output_data_tuple = (
                    export_lesson_data['дата'], export_lesson_data['тип'], export_lesson_data['аудитория'],
                    export_lesson_data['номер'], export_lesson_data['название'], export_lesson_data['преподаватель']
                )
                print(f"Data is written to the database (data: {output_data_tuple}).")
                # async with self.semaphore:
                #     await dbi.create_lesson(
                #         group_id=group_db.id,
                #         week_id=week_db.id,
                #         weekday=export_lesson_data["день недели"],
                #         date=export_lesson_data["дата"],
                #         number=export_lesson_data["номер"],
                #         start_time=export_lesson_data["начало"],
                #         title=export_lesson_data["название"],
                #         teacher=export_lesson_data["преподаватель"],
                #         type_=export_lesson_data["тип"],
                #         classroom=export_lesson_data["аудитория"])
                return
            except Exception as e:
                print(f"Warning: exception in line 286 of the _import_lesson method of the data_manager module."
                      f"Exception: {e}")
                return

        if lesson_classroom and lesson_type:
            # Разбиение по переносу строки
            lesson_name = lesson_name.splitlines()
            lesson_type = lesson_type.splitlines()
            lesson_classroom = lesson_classroom.splitlines()

            # Перебор переменных и создание отдельных пар
            for offset in range(0, len(lesson_name)):
                try:
                    lesson: dict = {"название" : "".join(lesson_name[offset].split(", ")[0:-1]),
                                    "преподаватель" : lesson_name[offset].split(", ")[-1],
                                    "тип" : lesson_type[offset],
                                    "аудитория" : lesson_classroom[offset]}
                    export_lesson_data.update(lesson)
                    output_data_tuple = (
                        export_lesson_data['дата'], export_lesson_data['тип'], export_lesson_data['аудитория'],
                        export_lesson_data['номер'], export_lesson_data['название'], export_lesson_data['преподаватель']
                    )
                    print(f"Data is written to the database {output_data_tuple}.")
                    # async with self.semaphore:
                    #     await dbi.create_lesson(
                    #         group_id=group_db.id,
                    #         week_id=week_db.id,
                    #         weekday=export_lesson_data["день недели"],
                    #         date=export_lesson_data["дата"],
                    #         number=export_lesson_data["номер"],
                    #         start_time=export_lesson_data["начало"],
                    #         title=export_lesson_data["название"],
                    #         teacher=export_lesson_data["преподаватель"],
                    #         type_=export_lesson_data["тип"],
                    #         classroom=export_lesson_data["аудитория"])
                except Exception as e:
                    print(f"##### Warning: exception in line 286 of the _import_lesson method of the data_manager module."
                          f"Exception: {e}")
                    continue