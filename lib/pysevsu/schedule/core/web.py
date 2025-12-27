import asyncio
import aiohttp
import requests

from aiohttp import ClientTimeout
from io import BytesIO
from bs4 import BeautifulSoup
from typing import Optional
from typing import Dict
from typing import Tuple
from typing import Final
from typing import Any

from .config import _URL
from .config import _COOKIES
from .config import _HEADERS
from ..utilites.logger import log


async def async_xls_request(url: str) -> BytesIO:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            resp = await response.read()
            return BytesIO(resp)


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

        self.bs4: BeautifulSoup = BeautifulSoup(_CONTENT, "html.parser")
        self.kw = kw

    async def run_data_stream(self):
        table = self.bs4.find('div', class_=Parser._SCHEDULE_TABLE)
        res: Dict[str, str] = dict()

        for e in table.descendants:
            if not hasattr(e, 'get'):
                continue

            classname = e.get("class")
            tag = e.name

            if tag == Parser._INSTITUTE_TAG:
                res["institute"] = e.get_text().strip()

            if classname:
                if Parser._STUDY_FORM_CLASS in classname:
                    res["study_form"] = e.get_text().strip()

                if Parser._SEMESTER_CLASS in classname:
                    res["semester"] = e.get_text().strip()

                if tag == Parser._URL_TAG:
                    res["excel_url"] = e.get("href").strip()

                if Parser._LINK_TITLE in classname:
                    res["course"] = e.get_text().strip()
                    yield res
                    
                    res.pop("semester", None)
                    res.pop("course", None)
                    res.pop("excel_url", None)

if __name__ == "__main__":
    parser = Parser()
    asyncio.run(parser.start())