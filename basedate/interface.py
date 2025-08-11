import asyncio
from typing import Optional
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from basedate.tables import *
import tracemalloc



DB_URL = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule"
engine = create_async_engine(DB_URL, echo=True)
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def create_group(name: str, 
                       course: Optional[int] = None, 
                       study_form: Optional[str] = None, 
                       institute: Optional[str] = None) -> Optional[Group]:
    async with AsyncSessionLocal() as session:
        # Проверка существования группы
        result = await session.execute(
            select(Group).where(
                Group.name == name,
                Group.course == course,
                Group.study_form == study_form,
                Group.institute == institute
            )
        )
        group = result.scalars().first()
        if not group:
            # Создаем новую группу
            group = Group(
                name=name, course=course, 
                study_form=study_form, 
                institute=institute
            )
            session.add(group)
            await session.commit()
            await session.refresh(group)
        return group

async def find_group(name: str, study_form: str, institute: str,
                     course: Optional[int] = None) -> Optional[Group]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Group).where(
                Group.name == name,
                Group.study_form == study_form,
                Group.institute == institute,
                Group.course == course
            )
        )
        return result.scalars().first()

async def create_week(number: int,
                      semester: Optional[str] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> Optional[Week]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Week).where(Week.number == number)
        )
        week = result.scalars().first()
        if not week:
            week = Week(
                number=number,
                semester=semester,
                start_date=start_date,
                end_date=end_date
            )
            session.add(week)
            await session.commit()
            await session.refresh(week)
        return week

async def find_week(number: int) -> Optional[Week]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Week).where(Week.number == number)
        )
        return result.scalars().first()

async def create_lesson(group_id: int, week_id: int, weekday: str,
                        date: str, number: int, start_time: str, title: str,
                        teacher: Optional[str] = None, type_: Optional[str] = None,
                        classroom: Optional[str] = None) -> Optional[Lesson]:
    async with AsyncSessionLocal() as session:
        lesson = Lesson(
            group_id=group_id,
            week_id=week_id,
            weekday=weekday,
            date=date,
            number=number,
            start_time=start_time,
            title=title,
            teacher=teacher,
            type_=type_,
            classroom=classroom
        )
        session.add(lesson)
        await session.commit()
        await session.refresh(lesson)
        return lesson

async def main():
    # Создаем таблицы (если ещё не созданы)
    await create_tables()

    # Создаём новую группу
    group = await create_group(
        name="Группа А",
        course=1,
        study_form="Очная",
        institute="Институт 1"
    )
    print(f"Создана группа: {group}")

    # Пытаемся найти ту же группу
    found_group = await find_group(
        name="Группа А",
        study_form="Очная",
        institute="Институт 1",
        course=1
    )
    print(f"Найдена группа: {found_group}")

    # Создаем неделю
    week = await create_week(
        number=1,
        semester="Весна",
        start_date="2024-02-01",
        end_date="2024-06-01"
    )
    print(f"Создана неделя: {week}")

    # Пытаемся найти эту неделю
    found_week = await find_week(number=1)
    print(f"Найдена неделя: {found_week}")

    # Создаем урок
    lesson = await create_lesson(
        group_id=group.id,
        week_id=week.id,
        weekday="Понедельник",
        date="2024-02-05",
        number=1,
        start_time="09:00",
        title="Математика",
        teacher="Иванов Иван Иванович",
        type_="Лекция",
        classroom="101"
    )
    print(f"Создан урок: {lesson}")

    
if __name__ == '__main__':
    asyncio.run(main())
