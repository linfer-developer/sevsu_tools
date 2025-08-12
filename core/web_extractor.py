import asyncio
import aiohttp
import requests

from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from typing import ClassVar, Optional

from core.excel_processor import ExcelFile
from core.config import URL, cookies, headers


class Extractor:

    SELECTOR_STUDY_FORM_COLUMN: ClassVar[str] = ".schedule-table__column" 
    SELECTOR_STUDY_FORM_HEADER: ClassVar[str] = ".schedule-table" \
                                                "__column-header" 
    SELECTOR_INSTITUTE_ACCORDION: ClassVar[str] = ".schedule-table" \
                                                  "__column-body > " \
                                                  ".document-accordion" 
    SELECTOR_INSTITUTE_HEADER: ClassVar[str] = ".document-accordion__header" \
                                               " > h4" 
    SELECTOR_SEMESTER_GROUP: ClassVar[str] = "div.document-link__group" 
    SELECTOR_SEMESTER_TITLE: ClassVar[str] = "h5" 
    SELECTOR_LINK: ClassVar[str] = "a" 

    def __init__(
        self,
        study_form: Optional[str] = None, 
        institute: Optional[str] = None,
        semester: Optional[str] = None,
        educational_degree: Optional[str] = None, 
        course: Optional[int] = None
    ):
        self.study_form = study_form 
        self.institute = institute 
        self.semester = semester 
        self.educational_degree = educational_degree 
        self.course = course 

        try: 
            response = requests.get(
                url=URL, 
                cookies=cookies, 
                headers=headers
            )
            self.HTML: str = BeautifulSoup(response.text, "html.parser")
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
        return self.HTML.select(Extractor.SELECTOR_STUDY_FORM_COLUMN)

    def study_form_title(
        self, 
        study_form_element: BeautifulSoup
    ) -> BeautifulSoup:
        return study_form_element.select(
            Extractor.SELECTOR_STUDY_FORM_HEADER
        )[0].text.strip() 

    def institutes_elements(
        self, 
        study_form_element: BeautifulSoup
    ) -> BeautifulSoup:
        return study_form_element.select(
            Extractor.SELECTOR_INSTITUTE_ACCORDION)

    def institute_title(
        self, 
        institute_element: BeautifulSoup
    ) -> BeautifulSoup:
        return institute_element.select(
            Extractor.SELECTOR_INSTITUTE_HEADER
        )[0].text.strip()

    def semestr_excels_list(
        self, 
        institute_element: BeautifulSoup
    ) -> BeautifulSoup:
        return institute_element.find_all(
            "div", class_="document-link__group")

    def find(self):
        asyncio.run(self.iterate_study_form())

    async def iterate_study_form(self) -> Optional[list]:
        tasks: list = []
        excel_file_website_path = ""
        
        for study_form_element in self.study_form_columns:

            study_form_title = self.study_form_title(study_form_element)
            institutes_elements = self.institutes_elements(study_form_element)

            if not self.study_form or self.study_form == study_form_title:
                excel_file_website_path += study_form_title

                task = asyncio.create_task(self.iterate_institute(
                    institutes_elements=institutes_elements, 
                    excel_file_website_path=excel_file_website_path)
                )
                print(
                    f"{id(task)}. Data collection from the student "
                    f"form \"{study_form_title}\" " 
                    f"has been started in the iterate_study_form " \
                    f"method of the web_extractor module.\n"
                )
                tasks.append(task)

            excel_file_website_path = excel_file_website_path.replace(
                study_form_title, ""
            )

        await asyncio.gather(*tasks)

    async def iterate_institute(
        self, 
        institutes_elements: BeautifulSoup,
        excel_file_website_path: str
    ) -> Optional[list]:
        tasks: list = []

        for institute_element in institutes_elements:
    
            institute_title = self.institute_title(institute_element)
            semestr_excels_list = self.semestr_excels_list(institute_element)

            if not self.institute or self.institute == institute_title:
                excel_file_website_path += f"/{institute_title}"

                task = asyncio.create_task(self.iterate_semesters(
                    semestr_excels_list=semestr_excels_list, 
                    excel_file_website_path=excel_file_website_path
                ))
                print(
                    f"{id(task)}. Data collection was started from "
                    f"the html element of the institute "
                    f"\"{institute_title}\" in the iterate_institute "
                    f"method of the web_extractor module.\n"
                )
                tasks.append(task)

            excel_file_website_path = excel_file_website_path.replace(
                f"/{institute_title}", ""
            )

        await asyncio.gather(*tasks)

    async def iterate_semesters(
        self, 
        semestr_excels_list: list,
        excel_file_website_path: str
    ) -> Optional[list]:
        tasks: list = []

        for semester in semestr_excels_list:

            semester_title = semester.find("div").text.strip()
            if not self.semester or self.semester == semester_title:
                excel_file_website_path += f"/{semester_title}"

                task = asyncio.create_task(self.iterate_links(
                    semester=semester, 
                    excel_file_website_path=excel_file_website_path)
                )
                print(
                    f"   {id(task)}. Data collection has started "
                    f"from the html element of the semester "
                    f"\"{semester_title}\" in the iterate_semesters " \
                    f"method of the web_extractor module."
                )
                tasks.append(task)

            excel_file_website_path = excel_file_website_path.replace(
                f"/{semester_title}", ""
            )

        await asyncio.gather(*tasks)

    async def iterate_links(
        self, 
        semester: BeautifulSoup, 
        excel_file_website_path: str
    ) -> list:
        tasks: list = []

        for link in semester.find_all("a"):
            link_name = link.text.strip()
            excel_file_website_path += f"/{link_name}"

            if not self.link_name or self.link_name == link_name:
                url = f"https://sevsu.ru{link.get('href')}"
                print(f"The collection of links to Excel files " \
                      f"has started in the iterate_links method "
                      f"of the web_extractor module. " \
                      f"Request attempt to {url} ({link_name})")

                try:
                    async with aiohttp.ClientSession() as session:
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
                                website_path=excel_file_website_path
                            )
                        ); tasks.append(task)

                except Exception as Error:
                    print(f"Предупреждение об исключении в "
                          f"строке 201: {Error}\n"
                          f"Путь до файла на сайте "
                          f"СевГУ: {excel_file_website_path}\n"
                          f"URL по которому произашло исключение: {url}\n")

            excel_file_website_path = excel_file_website_path.replace(
                f"/{link_name}", ""
            )

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

