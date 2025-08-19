import asyncio

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

from database.tables import *

DB_URL = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule"
engine = create_async_engine(
    url=DB_URL, 
    pool_size=20,
    max_overflow=40,
    echo=True
)
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def add_week(data: dict) -> None:
    async with AsyncSessionLocal() as session:
        async with session.begin():
            
            item = await session.execute(
                select(Week).where(
                    Week.start_date == data["week"]["start_date"],
                    Week.end_date == data["week"]["end_date"],
                    Week.semester == data["week"]["semester"],
                    Week.number == data["week"]["number"]
                )
            )

            if not item.scalars().first():
                session.add(
                    Week(
                        start_date=data["week"]["start_date"],
                        end_date=data["week"]["end_date"],
                        semester=data["week"]["semester"],
                        number=data["week"]["number"]
                    )
                )

async def add_group(data: dict) -> None:
    async with AsyncSessionLocal() as session:
        async with session.begin():

            item = await session.execute(
                select(Group).where(
                    Group.name == data["group"]["name"],
                    Group.course == data["group"]["course"],
                    Group.study_form == data["group"]["study_form"],
                    Group.institute == data["group"]["institute"]
                )
            )
            obj = item.scalars().first()

            if not obj: 
                session.add(
                    Group(
                        name=data["group"]["name"], 
                        course=data["group"]["course"], 
                        study_form=data["group"]["study_form"], 
                        institute=data["group"]["institute"]
                    )
                )

async def add_lesson(data: dict) -> None:
    async with AsyncSessionLocal() as session:
        async with session.begin():

            week_item = await session.execute(
                select(Week).where(
                    Week.start_date == data["week"]["start_date"],
                    Week.end_date == data["week"]["end_date"],
                    Week.semester == data["week"]["semester"],
                    Week.number == data["week"]["number"]
                )
            )
            week_obj = week_item.scalars().first()
            print(week_obj)

            group_item = await session.execute(
                select(Group).where(
                    Group.name == data["group"]["name"],
                    Group.course == data["group"]["course"],
                    Group.study_form == data["group"]["study_form"],
                    Group.institute == data["group"]["institute"]
                )
            )
            group_obj = group_item.scalars().first()
            print(group_obj)

            lesson_item = await session.execute(
                select(Lesson).where(
                    Lesson.group_id == group_obj.id,
                    Lesson.week_id == week_obj.id,
                    Lesson.weekday == data["lesson"]["weekday"],
                    Lesson.date == data["lesson"]["date"],
                    Lesson.number == data["lesson"]["number"],
                    Lesson.start_time == data["lesson"]["start_time"],
                    Lesson.title == data["lesson"]["title"],
                    Lesson.teacher == data["lesson"]["teacher"],
                    Lesson.type_ == data["lesson"]["type"],
                    Lesson.classroom == data["lesson"]["classroom"],
                )
            )
            lesson_obj = lesson_item.scalars().first()

            if not lesson_obj: 
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

if __name__ == '__main__':
    asyncio.run(create_tables())