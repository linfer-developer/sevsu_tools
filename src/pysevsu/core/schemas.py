from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class RequiredDataFields:
    study_year: str = "study_year"
    study_form: str = "study_form"
    institute: str = "institute"
    semester: str = "semester"
    full_course_name: str = "full_course_name"
    week_number: str = "week_number"
    week_start_date: str = "week_start_date"
    week_end_date: str = "week_end_date"
    group: str = "group"
    lesson_day: str = "lesson_day"
    lesson_date: str = "lesson_date"
    lesson_number: str = "lesson_number"
    lesson_start_time: str = "lesson_start_time"
    lesson_title: str = "lesson_title"
    lesson_teacher: str = "lesson_teacher"
    lesson_type: str = "lesson_type"
    lesson_classroom: str = "lesson_classroom"
