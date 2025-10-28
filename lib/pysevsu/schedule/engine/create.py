import asyncio
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
from ..core.web import sync_xls_request
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

class Process:

    def __init__(
        self, 
        generator_type: str, 
        *args: Callable, 
        **kwargs
    ):
        self.generator_type = generator_type
        self.args = args

        for key, value in kwargs.items():
            if key in PARAMS:
                setattr(self, key, value)

    @log
    async def gen_data(self):
        web_parser: Parser = Parser()
        tasks: List[Coroutine] = list()

        async for web in web_parser.start():
            url: str = rf"https://www.sevsu.ru{web['excel_url']}"
            await self.sheet_an(url, web.copy())

        await asyncio.gather(*tasks)

    @log
    async def sheet_an(self, url, web):
        tasks: List[Coroutine] = list()

        xls_content = await async_xls_request(url)
        xls = ExcelFile(xls_content) 

        async for sheet in xls.async_generate_schedule_worksheets():
            week: table.Week = table.Week(
                year=int(sheet.get_dates_of_the_week()["start_date"].split(".")[-1]),
                semester=dict_value(web, "semester", "nothing"),
                title=sheet.title,
                start_date=sheet.get_dates_of_the_week()["start_date"],
                end_date=sheet.get_dates_of_the_week()["end_date"]
            )
            task = asyncio.create_task(self.sheet_an_2(
                sheet, 
                web["study_form"], 
                web["institute"], 
                web["course"], 
                week
            ))
            tasks.append(task)
        
        await asyncio.gather(*tasks)

    @log
    async def sheet_an_2(
        self, 
        sheet: Worksheet, 
        study_form: Optional[str] = None, 
        institute: Optional[str] = None, 
        course: Optional[str] = None, 
        week: Optional[str] = None
    ) -> ...:
        async for lesson in sheet.generate_lessons():
            group: table.Group = table.Group(
                name=lesson["Группа"],
                course=course,
                institute=institute
            )

            titles: List[str] = dict_value(lesson, "Занятие", "splitlines")
            types: List[str] = dict_value(lesson, "Тип", "splitlines")
            classrooms: List[str] = dict_value(lesson, "Аудитория", "splitlines")

            for index in range(len(titles)):
                title, teacher = split_title(dict_value(titles, index, "strip"))
                lesson: table.Lesson = table.Lesson(
                    study_form=study_form,
                    group=group,
                    week=week,
                    weekday=dict_value(lesson, "День", "nothing"),
                    date=dict_value(lesson, "Дата", "nothing"),
                    number=dict_value(lesson,"№занятия", "nothing"),
                    start_time=dict_value(lesson, "Время", "nothing"),
                    title=title,
                    teacher=teacher,
                    type_=dict_value(types, index, "strip"),
                    classroom=dict_value(classrooms, index, "strip")
                )
                print(lesson.title, group.name)

def dict_value(
    dict_: Optional[Dict[Any, Any]], 
    value: Any, 
    action_type: Optional[str] = ...
    ):
    try:
        if dict_[value]:
            if action_type == "nothing":
                return dict_[value]
            if action_type == "strip":
                return dict_[value].strip()
            if action_type == "splitlines":
                return dict_[value].strip().splitlines()
    except:
        return "nothing"

def split_title(text: List[str]):
    title: str = ...
    teacher: str = ...
    if len(text.split(', ')) <= 1:
        title = text
    else:
        title  = ' '.join(text.split(', ')[0:-1])
        teacher = str(text.split(', ')[-1])
    return title, teacher

if __name__ == "__main__":
    engine: Process = Process(
        generator_type="we123",
    )
    asyncio.run(engine.gen_data())
