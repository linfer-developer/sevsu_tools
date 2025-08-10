import asyncio
import aiohttp
import requests

from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from typing import ClassVar, Optional

from basedate.interface import DatabaseInterface as dbi
from core.excel_processor import ExcelFile
from core.config import URL, cookies, headers


class Extractor:

    """Класс Extractor предоставляет инструменты
    для работы со страницей, где хранится расписание СевГУ в
    Excel формате.

    Суть его работы заключается в парсинге веб-страницы и получения
    оттуда необходимых данных, которые могут использоваться другими
    для скачивания/чтения Excel файлов расписания.

    Является самостоятельным классом, но в рамках моей реализации
    работает в композиции с модулями ExcelFile и UniversityShedule.
    """
    SELECTOR_STUDY_FORM_COLUMN: ClassVar[str] = ".schedule-table__column" # Колонка студ. формы
    SELECTOR_STUDY_FORM_HEADER: ClassVar[str] = ".schedule-table__column-header" # Название колонки студ. формы
    SELECTOR_INSTITUTE_ACCORDION: ClassVar[str] = ".schedule-table__column-body > .document-accordion" # Интститут элемент
    SELECTOR_INSTITUTE_HEADER: ClassVar[str] = ".document-accordion__header > h4" # Интитут название
    SELECTOR_SEMESTER_GROUP: ClassVar[str] = "div.document-link__group" # Список ссылок на расписание
    SELECTOR_SEMESTER_TITLE: ClassVar[str] = "h5" # Заголовок чего-либо 
    SELECTOR_LINK: ClassVar[str] = "a" # Ссылка на что-либо

    def __init__(
        self,
        study_form: Optional[str] = None, # Cтуд. форма
        institute: Optional[str] = None, # Институт
        semester: Optional[str] = None, # Семестр
        educational_degree: Optional[str] = None, # Академический уровень
        course: Optional[int] = None # Номер курса
    ):

        self.study_form = study_form 
        self.institute = institute 
        self.semester = semester 
        self.educational_degree = educational_degree 
        self.course = course 

        if not(isinstance(self.course, int)) and self.course is not None:
            raise TypeError("Аргумент course может быть только целочисленным типом (int)! ")

        try: 
            response = requests.get(url=URL, cookies=cookies, headers=headers)
            self.HTML: str = BeautifulSoup(response.text, "html.parser")
        except Exception as Error: 
            raise ConnectionError(f"Произошла ошибка подключения к {URL}. Текст ошибки: \n{Error}")

    @property
    def link_name(self) -> Optional[str]:
        try:
            return f"{self.course} курс {self.educational_degree.title()}"
        except:
            return None

    @property
    def study_form_columns(self) -> BeautifulSoup:
        return self.HTML.select(Extractor.SELECTOR_STUDY_FORM_COLUMN)

    def study_form_title(self, study_form_element: BeautifulSoup) -> BeautifulSoup:
        """Возращает название колонки студ. формы.

        :param study_form_element кусок html
        кода для выборки названия.
        :type study_form_element не ебу честно, не проверял,
        потом напишу, скорее всего объект или строка. 
        """
        return study_form_element.select(
               Extractor.SELECTOR_STUDY_FORM_HEADER)[0].text.strip() 

    def institutes_elements(self, study_form_element: BeautifulSoup) -> BeautifulSoup:
        """Возращает список институт-элементов основываясь
        на текущей колонки студ. формы.

        :param study_form_element кусок html кода для выборки названия
        :type study_form_element не ебу честно, не проверял,
        потом напишу, скорее всего объект или строка. 
        """
        return study_form_element.select(Extractor.SELECTOR_INSTITUTE_ACCORDION)
    
    def institute_title(self, institute_element: BeautifulSoup) -> BeautifulSoup:
        """Возращает список институт-элементов основываясь
        на текущей колонки студ. формы.

        :param study_form_element кусок html кода для выборки названия.
        :type study_form_element не ебу честно, не проверял, потом напишу, 
        скорее всего объект или строка. 
        """
        return institute_element.select(
               Extractor.SELECTOR_INSTITUTE_HEADER)[0].text.strip()

    def semestr_excels_list(self, institute_element: BeautifulSoup) -> BeautifulSoup:
        """Возращает список ссылок на Excel файл основываясь 
        на текущем институте.

        :param institute_element кусок html кода для выборки названия.
        :type institute_element не ебу честно, не проверял, потом напишу, 
        скорее всего объект или строка. 
        """
        return institute_element.find_all("div", class_="document-link__group")

    def find(self):
        asyncio.run(self.iterate_study_form())

    async def iterate_study_form(self) -> Optional[list]:
        tasks: list = []
        excel_file_website_path = ""
        
        # Перебор форм обучения (трех главных колонок) на странице сайта:
        for study_form_element in self.study_form_columns:

            # Получение заголовка(названия) колонки:
            study_form_title = self.study_form_title(study_form_element)
            # Получение списка интститутов итерируемой колонки для дальнейшего перебора:
            institutes_elements = self.institutes_elements(study_form_element)
            print(f"SUCCESS: data collection from the student form \"{study_form_title}\" " 
                  "has been started in the iterate_study_form method of the "
                  "web_extractor module.")

            if not self.study_form or self.study_form == study_form_title:
                excel_file_website_path += study_form_title
                task = asyncio.create_task(self.iterate_institute(institutes_elements, excel_file_website_path))
                tasks.append(task)

            excel_file_website_path = excel_file_website_path.replace(study_form_title, "")

        await asyncio.gather(*tasks)

    async def iterate_institute(
        self, 
        institutes_elements: BeautifulSoup,
        excel_file_website_path: str
    ) -> Optional[list]:
        tasks: list = []

        # Перебор институтов в итерируемой форме обучения:
        for institute_element in institutes_elements:
    
            # Получение заголовка(названия) института:
            institute_title = self.institute_title(institute_element)
            # Получение списка из элементов-семестров для дальнейшего перебора:
            semestr_excels_list = self.semestr_excels_list(institute_element)
            print("# SUCCESS: data collection was started from the html element "
                  f"of the institute \"{institute_title}\" in the iterate_institute method of the " 
                  "web_extractor module.")

            if not self.institute or self.institute == institute_title:
                excel_file_website_path += f"/{institute_title}"
                task = asyncio.create_task(self.iterate_semesters(semestr_excels_list, excel_file_website_path))
                tasks.append(task)

            excel_file_website_path = excel_file_website_path.replace(f"/{institute_title}", "")

        await asyncio.gather(*tasks)

    async def iterate_semesters(
        self, 
        semestr_excels_list: list,
        excel_file_website_path: str
    ) -> Optional[list]:
        tasks: list = []

        # Перебор элементов-семестров в итерируемом институте
        for semester in semestr_excels_list:

            semester_title = semester.find("div").text.strip()
            print(f"## Success: data collection has started from the html element " 
                  f"of the semester \"{semester_title}\" in the iterate_semesters "
                   "method of the web_extractor module.")

            if not self.semester or self.semester == semester_title:
                excel_file_website_path += f"/{semester_title}"
                task = asyncio.create_task(self.iterate_links(semester, excel_file_website_path))
                tasks.append(task)

            excel_file_website_path = excel_file_website_path.replace(f"/{semester_title}", "")

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
                print("### Success: The collection of links to Excel files has started "
                      "in the iterate_links method of the web_extractor module. " \
                     f"Request attempt to {url} ({link_name})")

                try:
                    async with aiohttp.ClientSession() as session:
                        print(f"### An attempt to get an Excel file ({url}) in the "
                               "iterate_links method of the web_extractor module.")
                        file: BytesIO = await self.file_request(session, url)
                        task = asyncio.create_task(self.import_excel_file_data(file, excel_file_website_path))
                        tasks.append(task)
                except Exception as Error:
                    print(f"Предупреждение об исключении в строке 201: {Error}\n",
                          f"Путь до файла на сайте СевГУ: {excel_file_website_path}\n",
                          f"URL по которому произашло исключение: {url}\n")

            excel_file_website_path = excel_file_website_path.replace(f"/{link_name}", "")

        await asyncio.gather(*tasks)

    @staticmethod
    async def file_request(session: object, url: str) -> BytesIO:
        async with session.get(url) as response:
            response.raise_for_status()
            data = BytesIO(await response.read())
            return data

    @staticmethod
    async def import_excel_file_data(
        file: BytesIO, 
        website_path: str
    ) -> Optional[list]: 
        """ Получение Excel файла в формате переменной с помощью запроса.

        :param url ccылка на Excel. 
        :type url str. """
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

