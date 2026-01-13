"""web.py

The web module provides tools for collecting data from the schedule page of
Sevastopol State University. Its use is limited within the core package.

Using a module outside the core is a bad practice, it is better to use the api.

TODO: Adjust the documentation to the standards.
"""

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

from src.pysevsu.core.config import COOKIES, HEADERS, URL


async def get_website_content() -> Optional[str]:
    """Getting the contents of the main schedule page of Sevastopol State
    University in string format. The timeout is 60 seconds.
    """
    async with aiohttp.ClientSession(cookies=COOKIES, headers=HEADERS) as session:
        async with session.get(URL, timeout=60) as response:
            response.raise_for_status()
            return await response.text()


@dataclass(frozen=True)
class Config:
    """A configuration dataclass containing css selectors and HTML markup tags
    that reflect the location of key data to be collected in the Document class.
    """

    head: str = "schedule-table__column-name"
    list_title: str = "h4"
    inlist_title: str = "document-link__group-name"
    filename: str = "document-link__name"
    file_url: str = "a"


@dataclass(frozen=True)
class DataFields:
    """A dataclass that stores the shared data keys of the module."""

    head: str = "head"
    list_title: str = "list_title"
    inlist_title: str = "inlist_title"
    filename: str = "filename"
    file_url: str = "file_url"


CONF = Config
FIELD = DataFields


class Document:
    """The Document class provides a method for collecting data from the HTML
    schedule page of Sevastopol State University.

    :param content: HTML structure in string format. Passed to bs4 as a class
                    argument.
    """

    def __init__(self, content: str) -> None:
        self._bs4: BeautifulSoup = BeautifulSoup(content, "html.parser")

    @staticmethod
    def _text(str_: str) -> str:
        return str_.get_text().strip()

    async def parse_all(self) -> AsyncIterator[Dict[str, str]]:
        """The generator method collects data from the magic strings specified
        in the Config dataclass. Returns a one-dimensional dictionary with the
        name of the fields specified in DataFields.
        The use of an asynchronous generator is due to the fact that the method
        is further used in an asynchronous environment.
        """
        tmp_states: Dict[str, str] = {}
        text = self._text

        for el in self._bs4.descendants:  # descendants is generator, which is faster.
            if hasattr(el, "get"):
                tag: str = el.name
                if CONF.list_title == tag:
                    tmp_states[FIELD.list_title] = text(el)
                elif CONF.file_url == tag:
                    tmp_states[FIELD.file_url] = el.get("href")

                classnames: List[str] = el.get("class")
                if classnames:
                    if CONF.head in classnames:
                        tmp_states[FIELD.head] = text(el)
                    elif CONF.inlist_title in classnames:
                        tmp_states[FIELD.inlist_title] = text(el)
                    elif CONF.filename in classnames:
                        tmp_states[FIELD.filename] = text(el)
                        # The file name is the last element of the nested list
                        # structure. That's why yield is here.
                        yield tmp_states


async def __test_document__parse_all(key: Optional[str] = "all") -> None:
    content: str = await get_website_content()
    doc: Document = Document(content)
    async for el in doc.parse_all():
        if key == "all":
            print(el)
        elif key == "xls" and not el["filename"].startswith("Распоряжение"):
            print(el)
        elif key == "orders" and el["filename"].startswith("Распоряжение"):
            print(el)


if __name__ == "__main__":
    asyncio.run(__test_document__parse_all("xls"))
