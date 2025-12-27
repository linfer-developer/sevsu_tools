"""
Модуль для работы с базой данных расписания с использованием SQLAlchemy и asyncio.

Этот модуль обеспечивает создание таблиц и асинхронное добавление данных в таблицы,
такие как недели, группы и уроки. В качестве ORM используется SQLAlchemy с асинхронным
подключением, а база данных — PostgreSQL.

Зависимости:
    - sqlalchemy
    - sqlalchemy.ext.asyncio
    - asyncio
    - typing

Константы:
    - DB_URL (str): URL подключения к базе данных.
    - engine (AsyncEngine): асинхронный движок SQLAlchemy.
    - AsyncSessionLocal (sessionmaker): фабрика сессий для работы с БД.
"""

import asyncio
from typing import Dict, Any
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from ..database.tables import *  # Импорт таблиц ORM

#: URL подключения к базе данных PostgreSQL с использованием asyncpg
DB_URL = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule"

#: Асинхронный движок SQLAlchemy
engine = create_async_engine(
    url=DB_URL,
    pool_size=20,
    max_overflow=40,
    echo=True
)

#: Фабрика асинхронных сессий SQLAlchemy
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def create_tables():
    """
    Создает все таблицы в базе данных, определенные в метаданных ORM модели.

    Использует асинхронный подключение к базе данных и вызывает метод
    `Base.metadata.create_all()` синхронно внутри асинхронной транзакции.

    :raises sqlalchemy.exc.SQLAlchemyError: при ошибках выполнения операции.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def add_week(data: dict) -> None:
    """
    Добавляет новую неделю в таблицу `Week`, если ее еще там нет.

    Проверяет наличие записи с совпадающими `start_date`, `end_date` и `number`.
    Если такой записи нет, создает новую.

    :param data: словарь с данными о неделе, должен содержать ключи:
        - start_date (date): дата начала недели
        - end_date (date): дата окончания недели
        - semester (str): семестр
        - number (int): номер недели

    :raises sqlalchemy.exc.SQLAlchemyError: при ошибках выполнения операции.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            session.add(
                Week(
                    start_date=data["start_date"],
                    end_date=data["end_date"],
                    semester=data["semester"],
                    number=data["number"]
                )
            )

async def add_group(data: dict) -> None:
    """
    Добавляет новую группу в таблицу `Group`, если ее еще нет.

    Проверяет наличие группы по имени, при отсутствии — создает новую.

    :param data: словарь с данными о группе, должен содержать ключи:
        - name (str): название группы
        - course (int): курс
        - institute (str): институт

    :raises sqlalchemy.exc.SQLAlchemyError: при ошибках выполнения операции.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            item = await session.execute(
                select(Group).where(Group.name == data["name"])
            )
            obj = item.scalars().first()
            if not obj:
                session.add(
                    Group(
                        name=data["name"],
                        course=data["course"],
                        institute=data["institute"]
                    )
                )

async def add_lesson(data: Dict[str, Any]) -> None:
    """
    Добавляет новое занятие (урок) в таблицу `Lesson`, если его еще нет.

    Перед добавлением ищет связанные объекты `Week` и `Group`. Если не удается найти
    связанные записи, операция завершается без добавления.

    :param data: словарь с данными урока, включает:
        - week_start_date (date): дата начала недели
        - week_end_date (date): дата окончания недели
        - number (int): номер недели
        - group (str): название группы
        - lesson (dict): словарь с деталями урока, включает:
            - weekday (int): день недели
            - date (date): дата урока
            - number (int): номер урока
            - start_time (time): время начала
            - title (str): название предмета
            - teacher (str): преподаватель
            - type (str): тип занятия
            - classroom (str): аудитория

    :raises sqlalchemy.exc.SQLAlchemyError: при ошибках выполнения операции.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Поиск связанной недели
            week_item = await session.execute(
                select(Week).where(
                    (Week.start_date == data["week_start_date"]) &
                    (Week.end_date == data["week_end_date"]) &
                    (Week.number == data["number"])
                )
            )
            week_obj = week_item.scalars().first()
            if not week_obj or not hasattr(week_obj, 'id'):
                return  # Неделя не найдена, выходим

            # Поиск связанной группы
            group_item = await session.execute(
                select(Group).where(Group.name == data["group"])
            )
            group_obj = group_item.scalars().first()
            if not group_obj:
                return  # Группа не найдена, выходим

            # Проверка существования урока
            lesson_item = await session.execute(
                select(Lesson).where(
                    (Lesson.group_id == group_obj.id) &
                    (Lesson.week_id == week_obj.id) &
                    (Lesson.weekday == data["lesson"]["weekday"]) &
                    (Lesson.date == data["lesson"]["date"]) &
                    (Lesson.number == data["lesson"]["number"]) &
                    (Lesson.start_time == data["lesson"]["start_time"]) &
                    (Lesson.title == data["lesson"]["title"]) &
                    (Lesson.teacher == data["lesson"]["teacher"]) &
                    (Lesson.type_ == data["lesson"]["type"]) &
                    (Lesson.classroom == data["lesson"]["classroom"])
                )
            )
            if not lesson_item.scalars().first():
                # Добавление нового урока
                session.add(
                    Lesson(
                        group_id=group_obj.id,
                        week_id=week_obj.id,
                        weekday=data["lesson"]["weekday"],
                        date=data["lesson"]["date"],
                        number=data["lesson"]["number"],
                        start_time=data["lesson"]["start_time"],
                        title=data["lesson"]["title"],
                        teacher=data["lesson"]["teacher"],
                        type_=data["lesson"]["type"],
                        classroom=data["lesson"]["classroom"]
                    )
                )

# В основном блоке запускается создание таблиц
if __name__ == '__main__':
    asyncio.run(create_tables())
