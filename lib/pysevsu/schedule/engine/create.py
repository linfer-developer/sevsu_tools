import asyncio
from datetime import datetime
from typing import Optional
from typing import Coroutine
from typing import Tuple
from typing import List
from typing import Callable
from typing import Dict
from typing import Any
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from ..core.web import Parser
from ..core.web import async_xls_request
from ..core.xls import ExcelFile
from ..core.xls import Worksheet
from ..database import tables as table
from ..utilites.logger import log


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
        await conn.run_sync(table.Base.metadata.create_all)


PARAMS: Tuple[str] = (
    "study_form", 
    "institute", 
    "semester", 
    "course", 
    "week", 
    "group"
)

async def requestbd(objects):
    async with AsyncSessionLocal() as session:
        async with session.begin():
            session.add_all(objects) 


class Process:

    def __init__(
        self, 
        generator_type: str, 
        *args: Callable, 
        **kwargs
    ):
        self.generator_type = generator_type
        self.args = args
        self._cache: Dict[Any] = dict()
        self._lock = asyncio.Lock()

        for key, value in kwargs.items():
            if key in PARAMS:
                setattr(self, key, value)

    @log
    async def gen_data(self):
        start = datetime.now()

        web_parser: Parser = Parser()
        tasks: List[Coroutine] = list()

        async for web in web_parser.start():
            if len(tasks) > 50:
                await asyncio.gather(*tasks)
                tasks.clear()
            url: str = rf"https://www.sevsu.ru{web['excel_url']}"
            task = asyncio.create_task(self.sheet_an(url, web.copy()))
            tasks.append(task)

        await asyncio.gather(*tasks)
        end = datetime.now()
        print(start, end)

    @log
    async def sheet_an(self, url, data):
        try:
            xls_content = await async_xls_request(url)
        except:
            return

        tasks: List[Coroutine] = list()
        db_tasks: List[Coroutine] = list()
        buffer: List[table.Week] = list()

        xls = ExcelFile(xls_content)
        async for sheet in xls.async_generate_schedule_worksheets():
            if not self._cache.get(sheet.title):
                week: table.Week = table.Week(
                    year=str(sheet.get_dates_of_the_week()["start_date"]).split(".")[-1],
                    semester=data.get("semester"),
                    title=sheet.title,
                    start_date=str(sheet.get_dates_of_the_week()["start_date"]),
                    end_date=str(sheet.get_dates_of_the_week()["end_date"])
                )
                buffer.append(week)
                self._cache[sheet.title] = week

            if len(buffer) >= 10:
                async with AsyncSessionLocal() as session:
                    async with session.begin():
                        session.add_all(buffer) 

            data.update({"week" : sheet.title})
            task = asyncio.create_task(self.sheet_an_2(sheet, data.copy()))
            tasks.append(task)
        
        if buffer:
            task = asyncio.create_task(requestbd(buffer))
            db_tasks.append(task)
            buffer.clear()
    
        await asyncio.gather(*tasks)
        await asyncio.gather(*db_tasks)

    @log
    async def sheet_an_2(
        self, 
        sheet: Worksheet, 
        data: Dict[Any, Any]
    ) -> ...:
        tasks: List[Coroutine] = list()
        db_tasks: List[Coroutine] = list()
        buffer: List[table.Week] = list()
        async for item in sheet.generate_lessons():
            if not self._cache.get(item["Группа"]):
                group: table.Group = table.Group(
                    name=item["Группа"],
                    course=data["course"],
                    institute=data["institute"]
                )
                self._cache[item["Группа"]] = group
                buffer.append(group)

            if len(buffer) >= 25:
                async with AsyncSessionLocal() as session:
                    async with session.begin():
                        session.add_all(buffer) 

            data.update(item)
            task = asyncio.create_task(self.sheet_an_3(data.copy()))
            tasks.append(task)

        if buffer:
            task = asyncio.create_task(requestbd(buffer))
            db_tasks.append(task)
            buffer.clear()
        
        await asyncio.gather(*tasks)
        await asyncio.gather(*db_tasks)

    @log
    async def sheet_an_3(self, data):
        buffer: List[table.Week] = list()
        db_tasks: List[Coroutine] = list()

        titles: Optional[List[str]] = data["Занятие"].splitlines()
        types: Optional[List[str]] = data["Тип"].splitlines()
        classrooms: Optional[List[str]] = data["Аудитория"].splitlines() 

        for index in range(len(titles)):
            title: str = None
            teacher: str = None
            if len(titles[index].split(', ')) <= 1:
                title = titles[index]
            else:
                title  = ' '.join(titles[index].split(', ')[0:-1])
                teacher = str(titles[index].split(', ')[-1])
            
            try:
                lesson: table.Lesson = table.Lesson(
                    study_form=data["study_form"],
                    group_id=self._cache[data["Группа"]].id,
                    week_id=self._cache[data["week"]].id,
                    weekday=data.get("День"),
                    date=data.get("Дата"),
                    number=int(data.get("№занятия")),
                    start_time=data.get("Время"),
                    title=title,
                    teacher=teacher,
                    type_=types[index] if types else "...",
                    classroom=classrooms[index] if classrooms else "..."
                )
            except:
                raise RuntimeError(types, classrooms)
            buffer.append(lesson)

            if len(buffer) >= 500:
                task = asyncio.create_task(requestbd(buffer))
                db_tasks.append(task)

        if buffer:
            task = asyncio.create_task(requestbd(buffer))
            db_tasks.append(task)
            buffer.clear()

        await asyncio.gather(*db_tasks)

if __name__ == "__main__":
    engine: Process = Process(
        generator_type="we123",
    )
    asyncio.run(engine.gen_data())
