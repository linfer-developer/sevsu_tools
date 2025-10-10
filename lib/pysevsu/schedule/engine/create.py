import asyncio
from typing import Optional
from typing import Coroutine
from typing import Tuple
from typing import List
from typing import Callable
from typing import Dict
from typing import Any
from ..core.web import Parser
from ..core.web import async_xls_request
from ..core.xls import ExcelFile
from ..core.xls import Worksheet
from ..utilites import callbacks


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

    async def gen_data(self):
        web_parser: Parser = Parser()
        tasks: List[Coroutine] = list()

        async for web in web_parser.start():
            url: str = rf"https://www.sevsu.ru{web["excel_url"]}"
            task = asyncio.create_task(self.sheet_an(url, web.copy()))
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def sheet_an(self, url, web):
        tasks: List[Coroutine] = list()

        try:
            xls = ExcelFile(await async_xls_request(url)) # Время на запрос
        except:
            return
        async for sheet in xls.async_generate_schedule_worksheets():
            try:
                week = sheet.get_dates_of_the_week() | {
                    "year" : int(sheet.get_dates_of_the_week()["start_date"].split(".")[-1]),
                    "title" : sheet.title,
                    "semester" : get_dict_format_value(web, "semester", "nothing")
                }
                task = asyncio.create_task(self.sheet_an_2(
                    sheet, 
                    web["study_form"], 
                    web["institute"], 
                    web["course"], 
                    week
                ))
                tasks.append(task)
            except RuntimeError:
                ...
        
        await asyncio.gather(*tasks)

    async def sheet_an_2(self, sheet, study_form, institute, course, week):
        async for lesson in sheet.generate_lessons():
            group = {
                "name" : lesson["Группа"],
                "course" : course,
                "institute" : institute
            }

            titles: List[str] = get_dict_format_value(
                lesson, 
                "Занятие",
                "splitlines"
            )
            types: List[str] = get_dict_format_value(
                lesson, 
                "Тип",
                "splitlines"
            )
            classrooms: List[str] = get_dict_format_value(
                lesson, 
                "Аудитория",
                "splitlines"
            )

            for index in range(len(titles)):
                title, teacher = split_title(
                    get_dict_format_value(
                        titles, 
                        index, 
                        "strip"
                    )
                )
                type_ = get_dict_format_value(
                    types, 
                    index, 
                    "strip"
                )
                classroom = get_dict_format_value(
                    classrooms, 
                    index, 
                    "strip"
                )

                data = {
                    "study_form" : study_form,
                    "group" : group,
                    "week" : week,
                    "weekday" : get_dict_format_value(
                        lesson, 
                        "День", 
                        "nothing"
                    ),
                    "date" : get_dict_format_value(
                        lesson, 
                        "Дата", 
                        "nothing"
                    ),
                    "number" : get_dict_format_value(
                        lesson,
                        "№занятия", 
                        "nothing"
                    ),
                    "start_time" : get_dict_format_value(
                        lesson, 
                        "Время", 
                        "nothing"
                    ),
                    "title" : title,
                    "teacher" : teacher,
                    "type" : type_,
                    "classroom" : classroom
                }
                print(data)

def get_dict_format_value(
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
        return None

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
