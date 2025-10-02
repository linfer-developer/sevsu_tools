from typing import overload
from typing import Callable
from typing import Optional
from typing import Any
from typing import List
from typing import Dict
from utilites import callbacks


class apm_xls:

    @overload
    def __init__(self, data):
        self.data = data

    @callbacks.create(
        type="parent",
        params=True,
        callbacks=True
    )
    def split_combined_lessons(
        self, 
        *args: Optional[Callable], 
        **kwargs: Optional[Any]
    ):
        """Обработка данных о занятиях, разделяя объединённые 
        уроки и вызывая указанные callbacks.

        Предполагается, что в `data` содержится информация о занятии, типе 
        и аудитории. Разделяет название занятия и преподавателя, обновляет 
        `data` и вызывает callbacks.

        :param data: словарь с данными о занятии.
        :param callbacks: функции-обработчики.
        
        """

        data = kwargs["data"]

        title_list: List[str] = data['Занятие'].strip().splitlines()
        type_list: List[str] = data['Тип'].strip().splitlines()
        classroom_list: List[str] = data['Аудитория'].strip().splitlines()

        for index in range(len(title_list)):
            if not title_list[index]:
                continue

            def split_title(text: List[str]):
                title: str = ''
                teacher: str = ''
                if len(text.split(', ')) <= 1:
                    title = text
                else:
                    title  = ' '.join(text.split(', ')[0:-1])
                    teacher = str(text.split(', ')[-1])
                return {'Занятие' : title, 'Преподаватель' : teacher}

            data.update(split_title(title_list[index]) | {
                'Тип' : type_list[index],
                'Аудитория' : classroom_list[index]
            })
            
            callbacks.start(data, *args)

    @callbacks.create(params=True)
    def convert_lesson_data_for_import(self, data):
        """Конвертация данных урока для импорта с переименованием ключей.

        Обновляет `data`, заменяя ключи русскими названиями на англоязычные,
        добавляет дополнительные данные и вызывает callbacks.

        :param data: исходные данные урока.
        :param callbacks: функции-обработчики.

        """

        new_keys: Dict[str, str] = {
            'Группа' : 'group', 'День' : 'weekday', 'Дата' : 'date',
            'Время' : 'time', '№занятия' : 'number', 'Занятие' : 'title',
            'Преподаватель' : 'teacher', 'Тип' : 'type', 
            'Аудитория' : 'classroom'
        }

        data = {
            new_keys.get(key, key): value
            for key, value in data.items()
        }
        data.update(self.additional_data)

    @callbacks.create(params=True)
    def convert_weeks_for_import(self, data) -> None:
        """Добавление информации о неделе и семестре в данные для импорта.

        Обновляет словарь `data`, добавляя номера 
        недели и семестр из `additional_data`.

        :param data: словарь с данными.

        """

        data.update({
            "week_number" : self.additional_data["week_number"],
            "semester" : self.additional_data["semester"]
        })

    @callbacks.create(params=True)
    def convert_groups_for_import(self, data) -> None:
        """Добавление информации о группе и курсе в данные для импорта.

        Обновляет словарь `data`, добавляя информацию 
        о институте и курсе из `additional_data`.

        :param data: словарь с данными.
        :return: None

        """

        data.update({
            "institute" : self.additional_data["institute"],
            "course" : self.additional_data["course"]
        })
