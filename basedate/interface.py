from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from basedate.tables import *

class DatabaseInterface:
    engine = create_engine(
        "postgresql://postgres:DrWend228@localhost:5432/schedule",
        pool_size=50, max_overflow=-1, pool_timeout=30)

    @classmethod
    def create_tables(cls):
        with cls.engine.begin() as conn:
            Base.metadata.create_all(bind=conn)

    @classmethod
    def create_group(cls, session: Session, name: str, 
                     course: Optional[int] = None, 
                     study_form: Optional[str] = None, 
                     institute: Optional[str] = None) -> Optional[Group]:
        res = session.execute(
            select(Group).where(
                Group.course == course,
                Group.study_form == study_form,
                Group.institute == institute,
                Group.name == name
            )
        )
        group = res.scalars().first()
        if not group:
            group = Group(
                name=name, course=course, 
                study_form=study_form, 
                institute=institute
            )
            session.add(group)
        return group

    @classmethod
    def find_group(cls, session: Session, name: str, study_form: str, institute: str,
                   course: Optional[int] = None) -> Optional[Group]:
        res = session.execute(
            select(Group).where(
                Group.course == course,
                Group.study_form == study_form,
                Group.institute == institute,
                Group.name == name
            )
        )
        return res.scalars().first()

    @classmethod
    def create_week(cls, session: Session, number: int,
                    semester: Optional[str] = None,
                    start_date: Optional[str] = None,
                    end_date: Optional[str] = None) -> Optional[Week]:
        res = session.execute(
            select(Week).where(Week.number == number)
        )
        week = res.scalars().first()
        if not week:
            week = Week(start_date=start_date, end_date=end_date, number=number, semester=semester)
            session.add(week)
        return week

    @classmethod
    def find_week(cls, session: Session, number: int) -> Optional[Week]:
        res = session.execute(
            select(Week).where(Week.number == number)
        )
        return res.scalars().first()

    @classmethod
    def create_lesson(cls, session: Session, group_id: int, week_id: int, weekday: str,
                      date: str, number: int, start_time: str, title: str,
                      teacher: Optional[str] = None, type_: Optional[str] = None,
                      classroom: Optional[str] = None) -> Optional[Lesson]:
        lesson = Lesson(
            group_id=group_id, week_id=week_id, weekday=weekday,
            date=date, number=number, start_time=start_time,
            title=title, teacher=teacher, type_=type_,
            classroom=classroom
        )
        session.add(lesson)
        return lesson