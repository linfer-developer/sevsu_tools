import asyncio
import aiohttp
import requests
from functools import wraps
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Callable
from typing import ClassVar
from typing import Optional
from core.excel_processor import ExcelFile
from core.queue_manager import ImportQueues
from core.config import URL, cookies, headers


def aiohttp_create_session(func: Callable):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with aiohttp.ClientSession() as session:
            return await func(session, *args, **kwargs)
    return wrapper

@aiohttp_create_session(url='https://example.com/file')
async def excel_file_request(session, url) -> Optional[BytesIO]:
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = BytesIO(await response.read())
            return data
    except Exception as e:
        print(f"Error: {e}")
        return None


class Engine:

    """The main purpose of the class is to parse and sequentially run 
    parallel schedule import tasks from Excel files to the database."""

    STUDY_FORM_COLUMN: ClassVar[str] = ".schedule-table__column" 
    STUDY_FORM_HEADER: ClassVar[str] = ".schedule-table__column-header" 
    INSTITUTE_ACCORDION: ClassVar[
                        str
                    ] = ".schedule-table__column-body > .document-accordion" 
    inst_HEADER: ClassVar[str] = ".document-accordion__header > h4" 
    SEMESTER_GROUP: ClassVar[str] = "div.document-link__group" 
    SEMESTER_TITLE: ClassVar[str] = "h5" 
    LINK: ClassVar[str] = "a" 

    def __init__(
        self,
        html: Optional[str],
        study_form: Optional[str] = None, 
        inst: Optional[str] = None,
        semester: Optional[str] = None,
        educational_degree: Optional[str] = None, 
        course: Optional[int] = None,
        echo: bool = False
    ):
        self.html = html
        self.study_form = study_form 
        self.inst = inst 
        self.semester = semester 
        self.educational_degree = educational_degree 
        self.course = course 
        self.echo = echo

        website_content = requests.get(
            url=URL, 
            cookies=cookies, 
            headers=headers
        ).text
        self.html: str = BeautifulSoup(website_content, "html.parser")

    @property
    def link_name(self) -> Optional[str]:
        try:
            return f"{self.course} курс {self.educational_degree.title()}"
        except:
            return None

    @property
    def study_form_columns(self) -> BeautifulSoup:
        return self.html.select(Engine.STUDY_FORM_COLUMN)

    def study_form_title(self, element: BeautifulSoup) -> BeautifulSoup:
        return element.select(Engine.STUDY_FORM_HEADER)[0].text.strip() 

    def institute_elements(self, element: BeautifulSoup) -> BeautifulSoup:
        return element.select(Engine.INSTITUTE_ACCORDION)

    def institute_title(self, element: BeautifulSoup) -> BeautifulSoup:
        return element.select(Engine.inst_HEADER)[0].text.strip() 

    def semestr_excels_list(self, element: BeautifulSoup) -> BeautifulSoup:
        return element.find_all("div", class_="document-link__group")

    def find(self):
        asyncio.run(self.iterate_study_form())

    async def iterate_study_form(self) -> Optional[list]:
        await ImportQueues.start()
        tasks: list = []
        web_path = ""

        for element in self.study_form_columns:
            title = self.study_form_title(element=element)
            institute_elements = self.institute_elements(element=element)

            if not self.study_form or self.study_form == title:
                web_path += title
                tasks.append(self.iterate_inst(
                    parent_element=institute_elements, 
                    web_path=web_path
                ))
            web_path = web_path.replace(title, "")

        await asyncio.gather(*tasks)
        await ImportQueues.stop()

    async def iterate_inst(
        self, 
        parent_element: BeautifulSoup,
        web_path: str
    ) -> Optional[list]:
        tasks: list = []

        for element in parent_element:
            title = self.institute_title(element=element)
            semestr_excels_list = self.semestr_excels_list(element=element)

            if not self.inst or self.inst == title:
                web_path += f"/{title}"
                tasks.append(asyncio.create_task(self.iterate_semesters(
                    parent_element=semestr_excels_list, 
                    web_path=web_path
                )))
            web_path = web_path.replace(f"/{title}", "")

        await asyncio.gather(*tasks)

    async def iterate_semesters(
        self, 
        parent_element: list,
        web_path: str
    ) -> Optional[list]:
        tasks: list = []

        for element in parent_element:
            title = element.find("div").text.strip()

            if not self.semester or self.semester == title:
                web_path += f"/{title}"
                tasks.append(asyncio.create_task(self.iterate_links(
                    parent_element=element, 
                    web_path=web_path
                )))
            web_path = web_path.replace(f"/{title}", "")

        await asyncio.gather(*tasks)

    async def iterate_links(
        self, 
        parent_element: BeautifulSoup, 
        web_path: str
    ) -> None:
        tasks: list = []

        for element in parent_element.find_all("a"):
            link = element.text.strip()
            web_path += f"/{link}"

            if not self.link_name or self.link_name == link:
                url = f"https://sevsu.ru{element.get('href')}"
                try:
                    async with aiohttp.ClientSession() as session:
                        tasks.append(asyncio.create_task(
                            self.import_excel_file_data(
                                file=await self.file_request(session=session, 
                                                             url=url), 
                                website_path=web_path
                            )
                        ))
                except Exception as e:
                    print(e)
            web_path = web_path.replace(f"/{link}", "")

        await asyncio.gather(*tasks)

    @staticmethod
    async def file_request(
        session: object, 
        url: str
    ) -> BytesIO:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = BytesIO(await response.read())
                return data
        except Exception as e:
            raise RuntimeError(e)

    @staticmethod
    async def import_excel_file_data(
        file: BytesIO, 
        website_path: str
    ) -> Optional[list]: 
        try:
            excel = ExcelFile(file=file, website_path=website_path)
            await excel.async_import()
        except IndexError:
            pass


if __name__ == "__main__":
    start = datetime.now()

    parser = Engine() 
    parser.find()

    end = datetime.now()
    print(start, end)

