import asyncio
import requests

from bs4 import BeautifulSoup
from typing import Dict
from typing import Final
from typing import Any

from .config import _URL
from .config import _COOKIES
from .config import _HEADERS


class Parser:
    _SCHEDULE_TABLE: Final[str] = 'schedule-table__content'
    _STUDY_FORM_CLASS: Final[str] = "schedule-table__column-name"
    _INSTITUTE_TAG: Final[str] = "h4"
    _SEMESTER_CLASS: Final[str] = "document-link__group-name"
    _LINK_TITLE: Final[str] = "document-link__name"
    _URL_TAG: Final[str] = "a"

    def __init__(self, **kw: Any):
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

        self._bs4: BeautifulSoup = BeautifulSoup(_CONTENT, "html.parser")
        self.kw = kw

    async def run_data_stream(self):
        table = self._bs4.find('div', class_=Parser._SCHEDULE_TABLE)
        res: Dict[str, str] = dict()

        for el in table.descendants:
            if not hasattr(el, 'get'):
                continue

            classname: str = el.get("class")
            tag: str = el.name

            match tag:
                case Parser._INSTITUTE_TAG:
                    res["institute"] = el.get_text().strip()
                case Parser._URL_TAG:
                    res["excel_url"] = el.get("href").strip()

            if classname:
                if Parser._STUDY_FORM_CLASS in classname:
                    res["study_form"] = el.get_text().strip()
                elif Parser._SEMESTER_CLASS in classname:
                    res["semester"] = el.get_text().strip()
                elif Parser._LINK_TITLE in classname:
                    res["course"] = el.get_text().strip()
                    yield res
                    
                    res.pop("semester", None)
                    res.pop("course", None)
                    res.pop("excel_url", None)

async def test():
    parser = Parser()
    count = 0
    async for button_info in parser.run_data_stream():
        count += 1
        print(count, button_info)

if __name__ == "__main__":
    asyncio.run(test())