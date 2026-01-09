class ScheduleParserError(Exception): ...


class SheetStructureError(ScheduleParserError): ...


class InvalidSheetSizeError(SheetStructureError):
    def __init__(self, actual_size: int, expected_min: int, sheet_name: str = ""):
        self.actual_size = actual_size
        self.expected_min = expected_min
        self.sheet_name = sheet_name
        message = (
            f"Лист '{sheet_name}' имеет {actual_size} строк. Минимум: {expected_min}."
        )
        super().__init__(message)


class InvalidHeaderError(SheetStructureError): ...


class DataValidationError(ScheduleParserError): ...
