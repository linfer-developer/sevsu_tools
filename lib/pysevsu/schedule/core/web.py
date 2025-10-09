import asyncio
import aiohttp
import requests
from io import BytesIO
from bs4 import BeautifulSoup
from typing import Dict
from typing import Tuple
from typing import Final
from typing import Any
from .config import _URL
from .config import _COOKIES
from .config import _HEADERS


# Executes an HTTP GET request to the main page of the site and receives the 
# page content.
try:
    _CONTENT: Final[str] = requests.get(
        url=_URL, 
        cookies=_COOKIES, 
        headers=_HEADERS
    ).text
except Exception as err:
    raise(
        ConnectionError(f"{err}.\nURL: {_URL}.")
    )

async def async_xls_request(url: str) -> BytesIO:
    """Asynchronously executes a GET request at the URL to an Excel file with 
    a schedule. Important, so that the request content is an xls table.

    :param url: The URL for the request.
    :type url: str
    :return: Processed response in the form of BytesIO.
    :rtype: BytesIO

    """
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return BytesIO(await response.read())


class Parser:

    # The schedule table includes elements of student forms, institutes, 
    # semesters, excel files. It is the parent element in relation to all 
    # the others.
    _SCHEDULE_TABLE: Final[str] = 'schedule-table__content'

    # The columns of the student forms, there are 3 of them in total, are 
    # visually distinguishable and have headings.
    _STUDY_FORM_CLASS: Final[str] = "schedule-table__column-name"

    # The institutes are clearly visually divided into blocks and have headings.
    # Each of the blocks is enclosed in one of the columns of the student form
    # and is mostly repeated.
    _INSTITUTE_TAG: Final[str] = "h4"

    # Semesters are available by clicking on the institute block and enclosed 
    # within it. 
    _SEMESTER_CLASS: Final[str] = "document-link__group-name"

    # Similarly to the semester, the names of the links are enclosed in the 
    # institute block and visually located in the same drop-down list with it.
    _LINK_TITLE: Final[str] = "document-link__name"

    # The link is in the `href`. The block that has a link is the parent of the 
    # link name.
    _URL_TAG: Final[str] = "a"

    def __init__(self, **kw: Any):
        """Initialization of the object :class'parser'."""

        FILTRED_PARAMS: Tuple[str] = (
            "study_form", 
            "institute", 
            "semester", 
            "course"
        )

        self.bs4: BeautifulSoup = BeautifulSoup(_CONTENT, "html.parser")
        self.filters: Dict[str, Any] = dict()

        for key, value in kw.items():
            if key in FILTRED_PARAMS and value:
                self.filters[key] = value

    def _apply_filter(self, data: Dict[str, Any]) -> bool:
        """Compares the occurrence of dictionaries."""
        
        return all(
            item in data.items() 
            for item in self.filters.items()
        )
    
    @property
    def available_filters(self) -> tuple:
        """Returns a tuple of available filters."""

        return ("study_form", "institute", "semester", "course")

    async def start(self):
        """Start collecting data from the website, implementing it through the 
        generator.

        The parsing is not based on the visual arrangement of the elements, but 
        on the syntactic analysis of the text (html code). This approach has 
        reduced the number of lines of code and nested loops (otherwise known 
        as methods).
        In the previous implementation, data collection took place as the html 
        blocks were asynchronously sorted: columns of student forms of study, 
        after blocks of institutes, lists of semesters and links were analyzed. 
        Accordingly, there were 4 nested asynchronously starting methods, which 
        then created many flexibility problems and violated one class-one 
        responsibility.

        Example::
            async for data in parser_obj.start():
                print(data)

        The result of executing this code, respectively, has the form:
        .. code-block:: text
            ...
            {'study_form': 'Расписание учебных занятий ОФО, ОЗФО', 'institute': 
            'Институт фундаментальной медицины и здоровьесбережения', 'semester'
            : 'I семестр', 'excel_url': '/univers/shedule/download.php?file=7Op
            7ZeoK%2F0G8NDi531o9QA%3D%3D', 'course': '2 курс Специалитет'}
            {'study_form': 'Расписание учебных занятий ОФО, ОЗФО', 'institute': 
            'Институт фундаментальной медицины и здоровьесбережения', 'semester'
            : 'I семестр', 'excel_url': '/univers/shedule/download.php?file=7Op7
            ZeoK%2F0G8NDi531o9QA%3D%3D', 'course': '3 курс Специалитет'}
            {'study_form': 'Расписание учебных занятий ОФО, ОЗФО', 'institute':
            'Институт фундаментальной медицины и здоровьесбережения', 'semest
            er': 'I семестр', 'excel_url': '/univers/shedule/download.php?fil
            e=YudoqW%2Bn5995XkPEBKmzfA%3D%3D', 'course': '3 курс Специалитет'}
            ...
        
        """
        
        table = self.bs4.find('div', class_=Parser._SCHEDULE_TABLE)
        data: Dict[str, str] = dict()

        for element in table.descendants:
            # If the get attributes do not exist, then the element is probably 
            # not a tag (it can be, for example, a comment)
            if not hasattr(element, 'get'):
                continue

            class_name = element.get("class")
            tag = element.name

            if class_name:
                # study form title
                if Parser._STUDY_FORM_CLASS in class_name:
                    data["study_form"] = element.get_text().strip()
                # semester title
                if Parser._SEMESTER_CLASS in class_name:
                    data["semester"] = element.get_text().strip()
                # link title
                if Parser._LINK_TITLE in class_name:
                    data["course"] = element.get_text().strip()
                # link url
                if tag == Parser._URL_TAG:
                    data["excel_url"] = element.get("href").strip()

            # institute title
            elif tag == Parser._INSTITUTE_TAG:
                try:
                    del (
                        data["semester"], 
                        data["course"], 
                        data["excel_url"]
                    )
                except KeyError:
                    ...
                finally:
                    data["institute"] = element.get_text().strip()
                
            # pre-filtering of data
            if len(data) == 5 and self._apply_filter(data):
                yield data

if __name__ == "__main__":
    parser = Parser()
    asyncio.run(parser.start())