# coding: utf-8
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Nivel1(Base):
    __tablename__ = 'nivel1'

    id = Column(Integer, primary_key=True)
    nome = Column(String(100), nullable=False)
    rg = Column(String(200), nullable=False)
    cpf = Column(String(200), nullable=False)
    idade = Column(String(200), nullable=False)
    data_de_nascimento = Column(Date)
    endereco = Column(String(200))
    email = Column(String(100))
    telefone = Column(String(50))
    profissao = Column(String(50))


class Nivel2(Base):
    __tablename__ = 'nivel2'

    id = Column(Integer, primary_key=True)
    nome = Column(String(100), nullable=False)
    rg = Column(String(200), nullable=False)
    cpf = Column(String(200), nullable=False)
    idade = Column(String(200), nullable=False)
    data_de_nascimento = Column(Date)
    endereco = Column(String(200))
    email = Column(String(100))
    telefone = Column(String(50))
    profissao = Column(String(50))
