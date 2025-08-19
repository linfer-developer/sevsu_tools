from typing import Optional
from sqlalchemy import ForeignKey, String, UniqueConstraint, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase): 
    def to_string(cls):
        print(cls)

class Lesson(Base):
    __tablename__ = 'lesson'

    id: Mapped[int] = mapped_column(primary_key=True) 

    group_id: Mapped[int] = mapped_column(ForeignKey('group.id'))
    week_id: Mapped[int] = mapped_column(ForeignKey('week.id'))

    group: Mapped["Group"] = relationship("Group", back_populates="lessons")
    week: Mapped["Week"] = relationship("Week", back_populates="lessons")
    weekday: Mapped[str] = mapped_column(String(15))
    date: Mapped[str] = mapped_column(String(15))
    number: Mapped[int] = mapped_column(Integer)
    start_time: Mapped[str] = mapped_column(String(15))
    title: Mapped[str] = mapped_column(String(200))
    teacher: Mapped[Optional[str]] = mapped_column(String(100))
    type_: Mapped[Optional[str]] = mapped_column(String(75))
    classroom: Mapped[Optional[str]] = mapped_column(String(75))

    __table_args__ = (UniqueConstraint(
        'date', 'group_id', 'week_id', 'weekday', 'number', 
        'start_time', 'title', 'teacher', 'type_', 'classroom',
        name='uix_lesson_unique'
    ),)

class Week(Base):
    __tablename__ = 'week'

    id: Mapped[int] = mapped_column(primary_key=True)

    number: Mapped[int] = mapped_column(Integer)
    start_date: Mapped[Optional[str]] = mapped_column(String(15))
    end_date: Mapped[Optional[str]] = mapped_column(String(15))
    semester: Mapped[Optional[str]] = mapped_column(String(15))

    lessons: Mapped[list["Lesson"]] = relationship("Lesson", back_populates="week") 

    __table_args__ = (UniqueConstraint(
        'number', 'start_date', 'end_date', 'semester', name='uix_week_unique'
    ),)

class Group(Base):
    __tablename__ = 'group'

    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str] = mapped_column(String(35))
    course: Mapped[Optional[str]] = mapped_column(String(55))
    study_form: Mapped[Optional[str]] = mapped_column(String(105))
    institute: Mapped[Optional[str]] = mapped_column(String(105))

    lessons: Mapped[list["Lesson"]] = relationship("Lesson", back_populates="group")

    __table_args__ = (UniqueConstraint(
        'name', 'course', 'study_form', 'institute', name='uix_group_unique'
    ),)