import asyncio
import aiohttp
import requests

from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from typing import ClassVar, Optional

from core.excel_processor import ExcelFile
from core.queue_manager import ImportQueues
from core.config import URL, cookies, headers

class Extractor:

    STUDY_FORM_COLUMN: ClassVar[str] = ".schedule-table__column" 
    STUDY_FORM_HEADER: ClassVar[str] = ".schedule-table__column-header" 
    inst_ACCORDION: ClassVar[str] = ".schedule-table__column-body > " \
                                                  ".document-accordion" 
    inst_HEADER: ClassVar[str] = ".document-accordion__header > h4" 
    SEMESTER_GROUP: ClassVar[str] = "div.document-link__group" 
    SEMESTER_TITLE: ClassVar[str] = "h5" 
    LINK: ClassVar[str] = "a" 

    def __init__(
        self,
        study_form: Optional[str] = None, 
        inst: Optional[str] = None,
        semester: Optional[str] = None,
        educational_degree: Optional[str] = None, 
        course: Optional[int] = None,
        echo: bool = False
    ):
        self.study_form = study_form 
        self.inst = inst 
        self.semester = semester 
        self.educational_degree = educational_degree 
        self.course = course 
        self.echo = echo

        try: 
            response = requests.get(
                url=URL, 
                cookies=cookies, 
                headers=headers
            )
            self.html: str = BeautifulSoup(response.text, "html.parser")
        except Exception as Error: 
            raise ConnectionError(f"Произошла ошибка подключения "
                                  f"к {URL}. Текст ошибки: \n{Error}")

    @property
    def link_name(self) -> Optional[str]:
        try:
            return f"{self.course} курс {self.educational_degree.title()}"
        except:
            return None

    @property
    def study_form_columns(self) -> BeautifulSoup:
        return self.html.select(Extractor.STUDY_FORM_COLUMN)

    def study_form_title(
        self, study_form_el: BeautifulSoup
    ) -> BeautifulSoup:
        return study_form_el.select(
            Extractor.STUDY_FORM_HEADER
        )[0].text.strip() 

    def inst_els(
        self, study_form_el: BeautifulSoup
    ) -> BeautifulSoup:
        return study_form_el.select(
            Extractor.inst_ACCORDION
        )

    def inst_title(
        self, inst_el: BeautifulSoup
    ) -> BeautifulSoup:
        return inst_el.select(
            Extractor.inst_HEADER
        )[0].text.strip() 

    def semestr_excels_list(
        self, inst_el: BeautifulSoup
    ) -> BeautifulSoup:
        return inst_el.find_all(
            "div", class_="document-link__group"
        )

    def find(self):
        asyncio.run(self.iterate_study_form())

    async def iterate_study_form(self) -> Optional[list]:
        tasks: list = []
        xls_path = ""

        await ImportQueues.start()
        for el in self.study_form_columns:
            title = self.study_form_title(study_form_el=el)
            inst_els = self.inst_els(study_form_el=el)

            if not self.study_form or self.study_form == title:
                xls_path += title

                task = asyncio.create_task(self.iterate_inst(
                    inst_els=inst_els, 
                    xls_path=xls_path
                ))

                if self.echo:
                    print(
                        f"{id(task)}. Data collection from the student "
                        f"form \"{title}\" " 
                        f"has been started in the iterate_study_form " \
                        f"method of the web_extractor module.\n"
                    )

                tasks.append(task)

            xls_path = xls_path.replace(title, "")

        await asyncio.gather(*tasks)
        await ImportQueues.stop()

    async def iterate_inst(
        self, 
        inst_els: BeautifulSoup,
        xls_path: str
    ) -> Optional[list]:
        tasks: list = []

        for el in inst_els:
            title = self.inst_title(inst_el=el)
            semestr_excels_list = self.semestr_excels_list(inst_el=el)

            if not self.inst or self.inst == title:
                xls_path += f"/{title}"

                task = asyncio.create_task(self.iterate_semesters(
                    semestr_excels_list=semestr_excels_list, 
                    xls_path=xls_path
                ))

                if self.echo:
                    print(
                        f"{id(task)}. Data collection was started from "
                        f"the html el of the inst "
                        f"\"{title}\" in the iterate_inst "
                        f"method of the web_extractor module.\n"
                    )

                tasks.append(task)

            xls_path = xls_path.replace(f"/{title}", "")

        await asyncio.gather(*tasks)

    async def iterate_semesters(
        self, 
        semestr_excels_list: list,
        xls_path: str
    ) -> Optional[list]:
        tasks: list = []

        for semester in semestr_excels_list:
            title = semester.find("div").text.strip()

            if not self.semester or self.semester == title:
                xls_path += f"/{title}"

                task = asyncio.create_task(self.iterate_links(
                    semester=semester, 
                    xls_path=xls_path
                ))

                if self.echo:
                    print(
                        f"{id(task)}. Data collection has started "
                        f"from the html el of the semester "
                        f"\"{title}\" in the iterate_semesters " \
                        f"method of the web_extractor module."
                    )

                tasks.append(task)

            xls_path = xls_path.replace(f"/{title}", "")

        await asyncio.gather(*tasks)

    async def iterate_links(
        self, 
        semester: BeautifulSoup, 
        xls_path: str
    ) -> list:
        tasks: list = []

        for link in semester.find_all("a"):
            link_name = link.text.strip()
            xls_path += f"/{link_name}"

            if not self.link_name or self.link_name == link_name:
                url = f"https://sevsu.ru{link.get('href')}"

                if self.echo:
                    print(
                        f"The collection of links to Excel files " \
                        f"has started in the iterate_links method "
                        f"of the web_extractor module. " \
                        f"Request attempt to {url} ({link_name})"
                    )

                try:
                    async with aiohttp.ClientSession() as session:
                        if self.echo:
                            print(f"An attempt to get an Excel file "
                                  f"({url}) in the iterate_links method " \
                                  f"of the web_extractor module.")

                        file: BytesIO = await self.file_request(
                            session=session, 
                            url=url
                        )
                        task = asyncio.create_task(
                            self.import_excel_file_data(
                                file=file, 
                                website_path=xls_path
                            )
                        )
                        tasks.append(task)

                except Exception as Error:
                    print(f"Предупреждение об исключении в "
                          f"строке 201: {Error}\n"
                          f"Путь до файла на сайте "
                          f"СевГУ: {xls_path}\n"
                          f"URL по которому произашло исключение: {url}\n")

            xls_path = xls_path.replace(f"/{link_name}", "")

        await asyncio.gather(*tasks)

    @staticmethod
    async def file_request(
        session: object, 
        url: str
    ) -> BytesIO:
        async with session.get(url) as response:
            response.raise_for_status()
            data = BytesIO(await response.read())
            return data

    @staticmethod
    async def import_excel_file_data(
        file: BytesIO, 
        website_path: str
    ) -> Optional[list]: 
        try:
            Excel = ExcelFile(file=file, website_path=website_path)
            await Excel.async_import()
        except IndexError:
            return

if __name__ == "__main__":
    start = datetime.now()

    parser = Extractor() 
    parser.find()

    end = datetime.now()
    print(start, end)

