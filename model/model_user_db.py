# coding: utf-8
from sqlalchemy import Column, Integer, Table, Text
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.ext.declarative import declarative_base
from controller import db, ma


class Nivel1(db.Model):
	__tablename__ = 'nivel1'
	id = db.Column(db.Integer, primary_key=True)
	line_hash = db.Column(db.Text)

class SchemaNivel1(ma.Schema):
	class Meta:
		fields = ('id', 'line_hash')
	ordered = True


