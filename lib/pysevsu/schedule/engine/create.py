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

        async for web in web_parser.start():
            url = rf"https://www.sevsu.ru{web["excel_url"]}"
            try:
                xls = ExcelFile(await async_xls_request(url))
            except:
                continue
            async for sheet in xls.async_generate_schedule_worksheets():
                async for data in sheet.generate_lessons():

                    titles: List[str] = get_dict_format_value(
                        data, 
                        "Занятие",
                        "splitlines"
                    )
                    types: List[str] = get_dict_format_value(
                        data, 
                        "Тип",
                        "splitlines"
                    )
                    classrooms: List[str] = get_dict_format_value(
                        data, 
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
