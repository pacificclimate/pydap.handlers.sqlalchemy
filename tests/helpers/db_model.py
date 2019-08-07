from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date
from sqlalchemy import DateTime, Boolean, ForeignKey, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Fruit(Base):
    __tablename__ = 'fruits'
    id = Column(Integer, primary_key=True)
    name = Column(String)


