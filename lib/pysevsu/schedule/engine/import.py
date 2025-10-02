from ..core.web import Parser
from ..core.xls import ExcelFile

async def import_():
    web_parser: Parser = Parser()
    async for item in web_parser.start():
        print(item)