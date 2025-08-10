
"""Обозначения, которые используются внутри модуля:
1) -> Зависимость в иеерархии при которой метод является вызываемым в следующем методе.
2) >>> Зависимость при которой метод, который вызывается создает логическую иерархию, но 
при этом вызывающий метод логически несвязный со следующей иерархией. Обычно это присуще 
глобальным методам сбора данных, иные случаи - плохая практика. 
3) () Отделение разных логических иерархий.
"""

import asyncio
from typing import Optional

class ScheduleParser:

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

    DATA_OUTPUT_PRESETS: tuple = ('пары', 'группы', 'недели', 'расписание')
    DETAILS_KEYS: tuple = ('export_to_database', 'filter_by_group_name', 'data_type')
    ADDITIONAL_DATA_KEYS: tuple = (
        'excel_file_worksheet', 'study_form', 'institute', 'semester', 'full_course_name')

    def __init__(self, data: list):
        self.data: list = data # Инициализированный ExcelFile
        self.details: dict = { # Детали и условия к получению данных
            'export_to_database' : False,
            'filter_by_group_name' : 'undefined ',
            'data_type' : 'расписание'
        }
        self.additional_data: dict = { # Дополнительные данные к импорту|экспорту
            'excel_file_worksheet' : 0, # Номер Excel листа
            'study_form' : 'undefined', # Студентческая форма
            'institute' : 'undefined', # Институт
            'semester' : 'undefined', # Семестр
            'full_course_name' : 'undefined' # Полное имя курса (1 курс бакалавриат)
        }

        if self.details['data_type'] not in ScheduleParser.DATA_OUTPUT_PRESETS:
            raise AttributeError(f"data_type может принимать только следующий значения: {self.DATA_OUTPUT_PRESETS} ")

        self.max_row = len(self.data)
        self.max_column = len(self.data[0])
        self.result: list = []

    def cell(self, row: int, col: int) -> Optional[str]:
        """Безопасно возвращает значение ячейки."""
        try:
            return self.data[row][col]
        except IndexError:
            return None

    async def async_fetch_data(self) -> Optional[list]:
        """Главный асинхронный метод для сбора данных с таблицы. 
        
        От этого методы следует иерархия: 
            x_iteration >>> (get_group_section -> get_day_section -> get_lesson_section)
        """
        if self.max_column < 40:
            return None

        row: int = 3 # С этого ряда начинается строка с группами

        self.result: list = await asyncio.gather(*[
            self.get_group_section(row, column)
            for column in range(0, self.max_column)
                if self.cell(row, column)])
        self.result = list(filter(None, self.result))

        return self.result

    async def get_group_section(
        self, 
        row: str, 
        column: str,
    ) -> Optional[dict]:
        """Метод для сбора данных с секции группы. 

        От метода следует иерархия: 
            get_group_section -> get_day_section -> get_lesson_section

        Реализация метода состоит в проверке на пустоты ячеек и сравнение с переданным
        фильтрами (если такие переданы). С помощью генератора в переменной study_days
        собирается список словарей дней, если те существуют (т.е. если есть пары в дне)
        
        :row str: ряд в Excel или первый индекс двумерном массиве в self.data
        :column str: столбце в Excel или второй индекс в двумерном массиве self.data
        """
        if self.cell(row, column):
            group: str = self.cell(row, column).split()[-1] # Шифр группы

            if self.details['data_type'] == 'группы':
                self.result.append(group)
                return

            # Если есть фильтр по группе, то проверяем это
            if self.details['filter_by_group_name'] != 'undefined' or self.details['filter_by_group_name'] == group:
                study_days: list = await asyncio.gather(*[ # Список словерей учебных дней
                    self.get_day_section(row+3+8*number, column) # Ячейка начала дня (день недели)
                    # Шаг 6 дней, т.к. в неделе 6 учебных дней максимум
                    for number in range(0, 6)]) 
                        # Процесс преобразования: row -> row+3 -> row+3+8*number.
                        # row: ряд групп.
                        # row+3: отступ 3 ряда вниз до дня недели.
                        # row+3+8*number: 8 - максимальное количество пар. 
                        #                 number - текущий день недели от 0.
                study_days = list(filter(None, study_days))

                if self.details['export_to_database']:
                    print(study_days)
                    course = int(self.additional_data['full_course_name'].split()[0])
                    study_form = self.additional_data['study_form']
                    institute = self.additional_data['institute']
                    export_group = {
                        'курс' : course,
                        'студ. форма' : study_form,
                        'институт' : institute,
                        'группа' : group
                    }

                if self.details['data_type'] == 'расписание':
                    if study_days:
                        return {group : study_days}

    async def get_day_section(
        self, 
        row: str, 
        column: str, 
    ) -> Optional[dict]:
        """Метод для сбора данных с секции дня.

        От метода следует иерархия: 
            get_day_section -> get_lesson_section
        
        Реализация метода состоит в сборе данных с ячеек названия дня, а затем в генерации 
        пар в текущем дне. В метод get_lesson_section в генерации передается текущий ряд и колонка.

        :row str: ряд в Excel или первый индекс двумерном массиве в self.data
        :column str: столбце в Excel или второй индекс в двумерном массиве self.data
        """
        weekday: str = self.cell(row, column) # День недели
        date: str = self.cell(row, column+1) # Дата дня

        if self.details['data_type'] == 'недели':
            self.result.append(date)
            return
        
        if self.details['export_to_database']:
            export_data = {
                'день недели' : weekday, 
                'дата' : date
            }

        lessons: list = await asyncio.gather(*[ # Список словарей секций пар
            self.get_lesson_section(row+offset, column+2, export_data) # Ячейка начала пары (номер пары)
            for offset in range(0, 8)]) # Шаг 8 пар, т.к. в день 8 пар максимум
                # row+offset - к ряду приповляется шаг
                # column+2 - от дня недели 2 ячейки до номера пары
        lessons = list(filter(None, lessons))

        if self.details['data_type'] == 'расписание':
            if lessons:
                return {'день недели' : weekday,
                        'дата' : date,
                        'пары' : lessons}

    async def get_lesson_section(
        self, 
        row: str = None, 
        column: str = None, 
        export_data: dict = False
    ) -> Optional[list]:
        """Метод для сбора данных с секции пары. Вызывается в методе get_day_section 
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
        lesson: list = []

        # Начало цикла с шагом 3 ячейки, именно столько составляет расстояние между разными парами.
        for count in [0, 3, 6]:
            # Если ячейка названия колонки пуста, то переходим к следующей итерации цикла.
            if not self.cell(5, column+count+2):
                continue

            column_title: str = self.cell(5, column+count+2).lower().strip() # Ячейка названия колонки
            if column_title in ["занятие", "тип", "аудитория"]:

                # Если ячейка название не пуста, то значение непустой ячейки записывается в переменную
                if self.cell(row, column+count+2):
                    lesson_name: str = self.cell(row, column+count+2).strip()

                # Если ячейки типа и аудитории не пусты, то значение непустой ячейки записывается в переменную
                if self.cell(row, column+count+3): # проверка только по ячейке типа, ибо тогда и другая существует также
                    lesson_type: str = self.cell(row, column+count+3).strip()
                    lesson_classroom: str = self.cell(row, column+count+4).strip()

                if lesson_name and lesson_type:
                    lesson_content = (lesson_name, lesson_type, lesson_classroom)

                    if self.details['export_to_database']:
                        export_data['номер'] = lesson_number
                        export_data['начало'] = lesson_start_time
                        task = asyncio.create_task(self.export_lesson(*lesson_content, export_data))
                        tasks.append(task)
                        continue

                    lessons = self.get_lessons(*lesson_content)
                    lesson = {
                        'номер' : lesson_number,
                        'начало' : lesson_start_time,
                        'занятия' : lessons
                    }

        await asyncio.gather(*tasks)

        if self.details['data_type'] == 'пары':
            self.result.append(lesson)

        if self.details['data_type'] == 'расписание':
            if lesson:
                return lesson

    @staticmethod
    def get_lessons(
        lesson_name: str,
        lesson_type: Optional[str] = None,
        lesson_classroom: Optional[str] = None,
    ) -> list:
        """Методы для сбора данных с ячейки, где более одной пары.

        Переменные разбиваются по переносу строки (по \n соответственно). Это сделано из-за того, что
        в Excel файле на одной паре может существоваться два и более предмета. 
        Например: элективные пары, разные группы по английскому, не связанные с основной учебной группой. 
        
        :lesson_number str: номер пары
        :lesson_start_time str: время начала пары
        :lesson_name str: название пары
        :lesson_type str: тип пары
        :lesson_classroom str: аудитория проведения пары
        """
        lesson: list = []

        if not lesson_classroom or not lesson_type:
            lesson.append({'занятие' : lesson_name,
                           'тип' : lesson_type,
                           'аудитория' : lesson_classroom})

        # Разбиение по переносу строки
        lesson_name = lesson_name.splitlines()
        lesson_type = lesson_type.splitlines()
        lesson_classroom = lesson_classroom.splitlines()

        # Перебор переменных и создание отдельных пар
        for offset in range(0, len(lesson_name)):
            try:
                lesson.append({'занятие' : lesson_name[offset],
                               'тип' : lesson_type[offset],
                               'аудитория' : lesson_classroom[offset]})
            except IndexError as Error:
                continue

        if lesson:
            return lesson
    
    @staticmethod
    async def export_lesson(
        lesson_name: str,
        lesson_type: Optional[str] = None,
        lesson_classroom: Optional[str] = None,
        export_data: Optional[str] = None
    ) -> None:
        if not lesson_classroom or not lesson_type:
            lesson = {'занятие' : lesson_name,
                    'тип' : lesson_type,
                    'аудитория' : lesson_classroom}
            lesson = export_data | lesson
            print(lesson)
            return
        
        # Разбиение по переносу строки
        lesson_name = lesson_name.splitlines()
        lesson_type = lesson_type.splitlines()
        lesson_classroom = lesson_classroom.splitlines()

        # Перебор переменных и создание отдельных пар
        for offset in range(0, len(lesson_name)):
            try:
                lesson = {'занятие' : lesson_name[offset],
                        'тип' : lesson_type[offset],
                        'аудитория' : lesson_classroom[offset]}
                lesson = export_data | lesson
                print(lesson)
            except IndexError as Error:
                continue
