"""Таблицы базы данных."""

from typing import Optional
from sqlalchemy import ForeignKey, String, UniqueConstraint, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase): 
    """Декларативный класс DeclarativeBase применяет процесс
    декларативного отображения ко всем подклассам."""
    def to_string(cls):
        print(cls)

class Lesson(Base):
    """Таблица пар. Входит: id, weekday, date, number, start_time, teacher, type, classroom."""
    __tablename__ = 'lesson'

    id: Mapped[int] = mapped_column(primary_key=True) # id
    # Ссылка на group.id и week.id в таблице group и week моделей Group и Weak
    group_id: Mapped[int] = mapped_column(ForeignKey('group.id')) # Внешний ключ на таблицу group
    week_id: Mapped[int] = mapped_column(ForeignKey('week.id')) # Внешний ключ на таблицу week
    # Установка отношений
    group: Mapped["Group"] = relationship("Group", back_populates="lessons") # Шифр группы
    week: Mapped["Week"] = relationship("Week", back_populates="lessons") # День недели
    weekday: Mapped[str] = mapped_column(String(15)) # День недели
    date: Mapped[str] = mapped_column(String(15)) # Дата
    number: Mapped[int] = mapped_column(Integer) # Номер (какая по счету пара)
    start_time: Mapped[str] = mapped_column(String(15)) # Время начала
    title: Mapped[str] = mapped_column(String(100)) # Название пары(предмета)
    teacher: Mapped[Optional[str]] = mapped_column(String(35)) # Преподаватель
    type_: Mapped[Optional[str]] = mapped_column(String(15)) # Тип пары(например лекция/практика/лабораторка/дистант)
    classroom: Mapped[Optional[str]] = mapped_column(String(15)) # Аудитория

    __table_args__ = (
        UniqueConstraint(
            'date', 'group_id', 'week_id', 'weekday', 'number', 'start_time',
            'title', 'teacher', 'type_', 'classroom',
            name='uix_lesson_unique'),)

class Week(Base):
    """Таблица учебных недель. Входит: id, start_date, end_date, number."""
    __tablename__ = 'week'

    id: Mapped[int] = mapped_column(primary_key=True) # id
    start_date: Mapped[Optional[str]] = mapped_column(String(15)) # Дата началы учебной недели
    end_date: Mapped[Optional[str]] = mapped_column(String(15)) # Дата конца учебной недели
    semester: Mapped[Optional[str]] = mapped_column(String(15)) # Дата началы учебной недели
    number: Mapped[int] = mapped_column(Integer) # Номер учебной недели
    lessons: Mapped[list["Lesson"]] = relationship("Lesson", back_populates="week") 

    __table_args__ = (UniqueConstraint('number', name='uix_week_unique'),)

class Group(Base):
    """Таблица студентческих групп. Входит: id, number, study_form, institute, group."""
    __tablename__ = 'group'

    id: Mapped[int] = mapped_column(primary_key=True) # id
    course: Mapped[Optional[int]] # Номер курса
    study_form: Mapped[Optional[str]] = mapped_column(String(105)) # Студентческая форма обучения
    institute: Mapped[Optional[str]] = mapped_column(String(105)) # Инстутут
    name: Mapped[str] = mapped_column(String(15)) # Шифр группы (ИТ/б-24-2-о)
    lessons: Mapped[list["Lesson"]] = relationship("Lesson", back_populates="group")

    __table_args__ = (UniqueConstraint('name', name='uix_group_unique'),)