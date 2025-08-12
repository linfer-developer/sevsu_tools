from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

from basedate.tables import *

DB_URL = "postgresql+asyncpg://postgres:DrWend228@localhost:5432/schedule"
engine = create_async_engine(
    url=DB_URL, 
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

async def add_week(week_obj: object) -> None:
    async with AsyncSessionLocal() as db_session:
        async with db_session.begin():
            if not await db_session.execute(
                select(Week).where(
                    Week.start_date == week_obj.start_date,
                    Week.end_date == week_obj.end_date,
                    Week.semester == week_obj.semester,
                    Week.number == week_obj.number
                )):
                    db_session.add(week_obj)

async def add_group(group_obj: object) -> None:
    async with AsyncSessionLocal() as db_session:
        async with db_session.begin():
            if not await db_session.execute(
                select(Group).where(
                    Group.name == group_obj.name,
                    Group.course == group_obj.course,
                    Group.study_form == group_obj.study_form,
                    Group.institute == group_obj.institute
                )): 
                    db_session.add(group_obj)

async def add_lesson(lesson_obj: object) -> None:
    async with AsyncSessionLocal() as db_session:
        async with db_session.begin():
            if not await db_session.execute(
                select(Lesson).where(
                    Lesson.group_id == lesson_obj.group_id,
                    Lesson.week_id == lesson_obj.week_id,
                    Lesson.weekday == lesson_obj.weekday,
                    Lesson.date == lesson_obj.date,
                    Lesson.number == lesson_obj.number,
                    Lesson.start_time == lesson_obj.start_time,
                    Lesson.title == lesson_obj.title,
                    Lesson.teacher == lesson_obj.teacher,
                    Lesson.type_ == lesson_obj.type_,
                    Lesson.classroom == lesson_obj.classroom,
                )): 
                    db_session.add(lesson_obj)
