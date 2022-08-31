from marshmallow import Schema

class SchemaNivel1(Schema):
	class Meta:
		fields = ('id','nome','rg','cpf','data_de_nascimento','endereco','email','telefone','profissao')
		ordered = True

class SchemaNivel2(Schema):
	class Meta:
		fields = ('id','nome','rg','cpf','data_de_nascimento','endereco','email','telefone','profissao')
		ordered = True

