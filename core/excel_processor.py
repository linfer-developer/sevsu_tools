import asyncio
import openpyxl

from typing import Optional
from core.data_manager import Importer
from basedate.interface import DatabaseInterface as dbi


class ExcelFile:

    """The ExcelFile class provides tools for working with Excel 
    a file from the SevSU schedule website.
    website url: https://www.sevsu.ru/univers/shedule/

    It is used for processing Excel files with the university schedule.

    Attributes
    ----------
    file: IOBytes
        IOBytes object (Excel file).
    website_path: str
        The path to the Excel file on the website with the SevGU schedule.

    Methods
    -------
    sheetnames()
        Returns a list of Excel file worksheets.
    async_import()
        Asynchronous import to the database. Creates 
        Worksheet class objects (a worker object 
        an Excel sheet of a file) and collects data from it.
    import_()
        Synchronous import to the database. Creates 
        Worksheet class objects (a worker object 
        an Excel sheet of a file) and collects data from it.
    """

    def __init__(self, file: object, website_path: str):
        self.file = file 
        self.website_path = website_path 
        self.excel = openpyxl.load_workbook(self.file, read_only=True)

    @property
    def sheetnames(self) -> list:
        """Returns a list of worksheet names."""
        return self.excel.sheetnames

    async def async_import(self):
        """Asynchronous import to the database. Creates 
        Worksheet objects and collects data from them.

        It is commonly used in other asynchronous functions. 
        and methods. 
        """
        await asyncio.gather(*[
            Worksheet(self, sheetname).import_()
            for sheetname in self.sheetnames
                if sheetname.startswith("уч.н.")])

    def import_(self, data_output) -> Optional[list]:
        asyncio.run(self.export_date_to_basedate(data_output))

class Worksheet:

    """The class provides tools for working with
    an Excel file worksheet.

    Used for caching data from the Excel worksheet 
    and their subsequent data processing of the 
    data_manager module.
    
    Attributes
    ----------
    excel: ExcelFile
        The created Excel file object (ExcelFile).
    worksheet_name: str
        The name of the worksheet that should be used 
        work must be done.

    Methods
    -------
    async_import()
        Asynchronous collection of cached data using 
        the data_manager module.
    import_()
        Synchronous collection of cached data using 
        the data_manager module.
    """

    def __init__(self, excel: ExcelFile, worksheet_name: str):
        self.excel = excel 
        self.worksheet_name = worksheet_name
        self.sheet = self.excel.excel[self.worksheet_name]
        self.worksheet_cache = self._load_cache()

    def _load_cache(self):
        """Caching data from an Excel file sheet."""
        return [[cell.value for cell in row] 
                    for row in self.sheet.rows]

    async def import_(self):
        additional_data = self.excel.website_path.split("/")
        worksheet_number = int(self.worksheet_name.split()[-1])
        importer = Importer(data=self.worksheet_cache)
        

        # Additional data to import
        importer.additional_data = {
            "week_number" : worksheet_number, # Excel worksheet number
            "study_form" : additional_data[0], # Study form
            "institute" : additional_data[1], # Institute
            "semester" : additional_data[2], # Semester
            "full_course_name" : additional_data[3] # Full name of the course
        }   

        await importer.async_import_data()
        print(f'Async import to {self.excel.website_path}. Excel worksheet: {self.worksheet_name}')
