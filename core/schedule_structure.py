""" Модуль представляет из себя структуру данных расписания. """

from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Group:

    name: str # Шифр группы 
    course: Optional[int] = None # Номер курса
    educational_degree: Optional[str] = None # Степень образования
    study_form: Optional[str] = None # Студентческая форма обучения
    institute: Optional[str] = None # Инстутут

    def __post_init__(self):
        if self.name is None:
            raise("Шифр группы не указан, работа со структурой Group невозможна. ")
        if self.course is None:
            self.course = self.name.split('-')[0][-1]
        if self.educational_degree is None:
            self.educational_degree = self.name.split('/')[-1] # ИТ/б-24-2-о

    def to_string(self):
        pass

@dataclass
class Week:
    week_number: Optional[str] = None # Номер недели
    start_date: Optional[str] = None
    days: Optional[List] = None # Массив объектов класса Day

@dataclass
class Lesson:

    week_number: Optional[int] = None # Номер недели
    group: Optional[str] = None # Шифр группы
    study_form: Optional[str] = None # Студентческая форма обучения
    institute: Optional[str] = None # Инстутут
    date: Optional[str] = None # Дата
    weekday: Optional[str] = None # День недели
    number: Optional[int] = None # Номер пары
    start_time: Optional[str] = None # Время начала пары
    name: Optional[List] = None # Названия пар на конкретной по счету паре
    teacher: Optional[List] = None # Названия пар на конкретной по счету паре
    type: Optional[List] = None # Типы пар на конкретной по счету паре
    classroom: Optional[List] = None # Аудитория пар на конкретной по счету паре

    def to_string(self) -> str:
        print("\n")
        print(f"{self.week_number:<3} | "
              f"{self.group.strip():<10} | "
              f"{self.study_form.strip():<10} | "
              f"{self.institute.strip():<10} | "
              f"{self.weekday.strip():<12} | "
              f"{self.date.strip():<12} | "
              f"{self.number:<8} | "
              f"{self.start_time.strip():<8} | "
              f"{self.name.strip():<20} | "
              f"{self.teacher.strip():<15} | "
              f"{self.type.strip():<5} | "
              f"{self.classroom.strip():<10}")