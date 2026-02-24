from typing import Optional

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase): ...


class Lesson(Base):
    __tablename__: str = "lesson"
    __table_args__: tuple = (
        UniqueConstraint(
            "study_form",
            "group_id",
            "week_id",
            "weekday",
            "date",
            "number",
            "start_time",
            "title",
            "teacher",
            "type_",
            "classroom",
            name="uix_lesson_unique"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    study_form: Mapped[Optional[str]] = mapped_column(String(105))
    group_id: Mapped[int] = mapped_column(ForeignKey("group.id"))
    week_id: Mapped[int] = mapped_column(ForeignKey("week.id"))
    weekday: Mapped[str] = mapped_column(String(15))
    date: Mapped[str] = mapped_column(String(15))
    number: Mapped[int] = mapped_column(Integer)
    start_time: Mapped[str] = mapped_column(String(15))
    title: Mapped[str] = mapped_column(String(200))
    teacher: Mapped[Optional[str]] = mapped_column(String(100))
    type_: Mapped[Optional[str]] = mapped_column(String(75))
    classroom: Mapped[Optional[str]] = mapped_column(String(75))

    group: Mapped["Group"] = relationship("Group", back_populates="lessons")
    week: Mapped["Week"] = relationship("Week", back_populates="lessons")


class Week(Base):
    __tablename__ = "week"
    __table_args__: tuple = (
        UniqueConstraint(
            "title", 
            "start_date", 
            "end_date", 
            name="uix_week_unique"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    year: Mapped[Optional[str]] = mapped_column(String(45))
    semester: Mapped[Optional[str]] = mapped_column(String(45))
    title: Mapped[str] = mapped_column(String(45))
    start_date: Mapped[Optional[str]] = mapped_column(String(45))
    end_date: Mapped[Optional[str]] = mapped_column(String(45))

    lessons: Mapped[list["Lesson"]] = relationship(
        "Lesson", back_populates="week"
    )


class Group(Base):
    __tablename__: str = "group"
    __table_args__: tuple = (
        UniqueConstraint("name", name="uix_group_unique"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(35))
    course: Mapped[Optional[str]] = mapped_column(String(65))
    institute: Mapped[Optional[str]] = mapped_column(String(105))

    lessons: Mapped[list["Lesson"]] = relationship(
        "Lesson", back_populates="group"
    )