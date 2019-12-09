from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from secret import db_from, db_to


print('~~~', datetime.now())
eng_from = create_engine('{connector}://{user}:{password}@{server}/{db}{additional}'.format(**db_from))
eng_to = create_engine('{connector}://{user}:{password}@{server}/{db}{additional}'.format(**db_to))
# eng_to = create_engine('sqlite:///:memory:')
SessionFrom = sessionmaker(bind=eng_from)
SessionTo = sessionmaker(bind=eng_to)
session_from = SessionFrom()
session_to = SessionTo()
Base = declarative_base()


class Departments(Base):
    __tablename__ = 'ext_departments'
    id = Column(Integer, primary_key=True)
    ext_id = Column(Integer)
    name = Column(String(255))
    chief_id = Column(Integer)
    employers = relationship('Employers', back_populates='department')

    def __init__(self, ext_id, name, chief_id):
        self.ext_id = ext_id
        self.name = name
        self.chief_id = chief_id


class Employers(Base):
    __tablename__ = 'ext_employers'
    id = Column(Integer, primary_key=True)
    ext_id = Column(Integer)
    name = Column(String(255))
    status = Column(String(255))
    department = relationship(Departments, back_populates='employers')
    department_id = Column(Integer, ForeignKey('ext_departments.id'), nullable=True)

    def __init__(self, ext_id, name, status, department):
        self.ext_id = ext_id
        self.name = name
        self.status = status
        self.department = department


# Employers.__table__.drop(eng_to, checkfirst=True)
# Departments.__table__.drop(eng_to, checkfirst=True)
Base.metadata.create_all(eng_to)
print('Clear tables')
# session_to.query(Employers).delete()
# session_to.query(Departments).delete()


class DataFrom(Base):
    __tablename__ = 'MBanalit'
    XRecID = Column(Integer, primary_key=True)
    NameAn = Column(String)  # ФИО
    Stroka = Column(String)  # Должность
    Podr = Column(Integer)  # Код отдела
    vid = Column(Integer)  # 288 - работники, 446 - отделы
    YesNo2 = Column(String)  # 'Д' - работники
    FIO = Column(Integer)  # id начальника отдела


print('departments')

department_ext = {}
for dep, ext_id, chief in session_from.query(DataFrom.NameAn, DataFrom.XRecID, DataFrom.FIO).filter(
        DataFrom.vid == 446):
    department_ext[ext_id] = Departments(name=dep, ext_id=ext_id, chief_id=chief)
department_int = [val for val, in session_to.query(Departments.ext_id)]

to_delete = set(department_int) - set(department_ext.keys())
session_to.query(Departments).filter(Departments.ext_id.in_(to_delete)).delete()

to_create = set(department_ext.keys()) - set(department_int)
to_create_val = [val for key, val in department_ext.items() if key in to_create]
session_to.add_all(to_create_val)


print('employers')
employer_ext = {}
for row in session_from.query(DataFrom).filter_by(vid=288).filter_by(YesNo2='Д'):
    department = department_ext.get(row.Podr)
    employer_ext[row.XRecID] = Employers(ext_id=row.XRecID, name=row.NameAn, status=row.Stroka, department=department)
employer_int = [val for val, in session_to.query(Employers.ext_id)]

to_delete = set(employer_int) - set(employer_ext.keys())
session_to.query(Employers).filter(Employers.ext_id.in_(to_delete)).delete()

to_create = set(employer_ext.keys()) - set(employer_int)
to_create_val = [val for key, val in employer_ext.items() if key in to_create]
session_to.add_all(to_create_val)


session_to.commit()
print('All OK')

# for row in eng_from.execute("select * from MBanalit where vid = 446"):
#     print([(k, v) for k, v in dict(row).items() if v])

# for row in eng_from.execute("select * from MBanalit where vid = 288 and YesNo2 = 'Д'"):
#     print([(k, v) for k, v in dict(row).items() if v])

# [
# ('StatFlag', '+'), ('Dop5', '1100'), ('Recv', '             Д002118'),
# ('XRecID', 106043), ('Stroka2', 'Astral.LGIP'),
# ('NameAn', 'Добрин Константин Георгиевич'), ('Date2', datetime.datetime(2000, 10, 2, 0, 0)), ('OriginalAn', 106043),
# ('Stroka', 'Начальник отдела'), ('OurFirm', 38838),
# ('Sost', 'Д'), ('IntNumber', 1128), ('XRecStat', '+'), ('Dop6', '214'),
# ('Vid', 288), ('Podr', 2184146), ('PostKind', 327672),
# ('YesNo', 'Д'), ('Persona', 106044), ('DatOpen', datetime.datetime(1981, 8, 15, 0, 0)),
# ('UniqueNameValue', 106043), ('LGIP_Ploschadka', 326347), ('Kod', '  НД000026'),
# ('Polzovatel', 106018),
# ('String', 'Konstantin@lgip.spb.ru'), ('Dop', 'Добрин Константин Георгиевич'), ('Dop3', '50018'), ('IsDouble', 'Н'),
# ('YesNo2', 'Д'), ('Analit', 106043),
# ('Prim', 'Не работает с:22.11.2019'), ('Data9', datetime.datetime(2019, 11, 22, 0, 0)), ('Dop4', '374-25-75'),
# ('Zdanie', '1')]

""" DB From:
employers: select * from MBanalit where vid = 288 and YesNo2 = 'Д'
departments: select * from MBanalit where vid = 446
NameAn - Фио
Stroka - Должность
Podr - Код отдела
"""
# /home/asofts/dbcopy_venv/bin/python /home/asofts/dbcopy/dbcopy.py