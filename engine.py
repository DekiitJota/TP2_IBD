#-*- coding: utf-8 -*-

import io
import sqlite3
import uuid
import csv

conn = sqlite3.connect('preco_medicamentos.sql')
cur = conn.cursor()

conn.executescript('''
    PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS PRODUTO (
        ID_PRODUTO INTEGER PRIMARY KEY, 
        NOME TEXT,
        CLASSE TEXT,
        TARJA TEXT,
        ID_REG INTEGER REFERENCES REGISTRO (ID_REG),
        ID_SUBSTANCIA INTEGER REFERENCES SUBSTANCIA (ID_SUBSTANCIA),
        ID_APRESENTACAO INTEGER REFERENCES APRESENTACAO (ID_APRESENTACAO),
        ID_TIPO INTEGER REFERENCES TIPO (ID_TIPO)
    );
    CREATE TABLE IF NOT EXISTS REGISTRO (
        ID_REG INTEGER PRIMARY KEY,
        EAN1 TEXT,
        PRECO_MAXIMO NUMERIC,
        COD_REGISTRO INTEGER
    );
    CREATE TABLE IF NOT EXISTS LABORATORIO (
        ID_LAB INTEGER PRIMARY KEY,
        NOME_LAB TEXT,
        CNPJ TEXT,
        ID_REG INTEGER REFERENCES REGISTRO (ID_REG)
    );
    CREATE TABLE IF NOT EXISTS SUBSTANCIA (
        ID_SUBSTANCIA INTEGER PRIMARY KEY,
	NOME TEXT
    ); 
    CREATE TABLE IF NOT EXISTS APRESENTACAO (
        ID_APRESENTACAO INTEGER PRIMARY KEY,
        DESCRICAO TEXT,
        COD_GGREM INTEGER
    );
    CREATE TABLE IF NOT EXISTS TIPO (
        ID_TIPO INTEGER PRIMARY KEY,
        STATUS TEXT
    );
    COMMIT;
''')

with open('test2.csv', 'r') as csv_file:
    contents = csv_file.read().decode("UTF-8")
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        substancia=row[0]
        cnpj=row[1]
        laboratorio=row[2]
        codigo_ggrem=row[3]
        registro=row[4]
        ean1=row[5]
        ean2=row[6]
        ean3=row[7]
        produto=row[8]
        descricao=row[9]
        classe=row[10]
        status=row[11]
        regime_preco=row[12]
        preco_fab=row[13] #preco de fabrica s/ impostos
        #preço de fabrica @ x% = Xpc
        preco_fab_0pc=row[14] 
        preco_fab_12pc=row[15]
        preco_fab_17pc=row[16]
        preco_fab_17pc_alc=row[17]
        preco_fab_17_5pc=row[18]
        preco_fab_17_5pc_alc=row[19]
        preco_fab_18pc=row[20]
        preco_fab_18pc_alc=row[21]
        preco_fab_20pc=row[22]
        #preço de mercado @ x% = Xpc
        preco_mc_0pc=row[23]
        preco_mc_12pc=row[24]
        preco_mc_17pc=row[25]
        preco_mc_17pc_alc=row[26]
        preco_mc_17_5pc=row[27]
        preco_mc_17_5pc_alc=row[28]
        preco_mc_18pc=row[29]
        preco_mc_18pc_alc=row[30]
        preco_mc_20pc=row[31]
        restricao_hosp=row[32]
        cap=row[33]
        confaz=row[34]
        icms=row[35]
        analise_recursal=row[36]
        cr_tributario=row[37]
        comercializacao=row[38]
        tarja=row[39]

id_produto = str(uuid.uuid4()).replace('-','')
id_reg = str(uuid.uuid4()).replace('-','')
id_lab = str(uuid.uuid4()).replace('-','')
id_substancia = str(uuid.uuid4()).replace('-','')
id_apresentacao = str(uuid.uuid4()).replace('-','')
id_tipo = str(uuid.uuid4()).replace('-','')

conn.executescript('''
    INSERT INTO PRODUTO(ID_PRODUTO,NOME,CLASSE,TARJA,ID_REG,ID_SUBSTANCIA,ID_APRESENTACAO,ID_TIPO)
        VALUES (?,?,?,?,?,?,?,?);
'''), (id_produto,produto,classe,tarja,id_reg,id_substancia,id_apresentacao,id_tipo)

conn.executescript('''
    INSERT INTO REGISTRO(ID_REG,EAN1,PRECO_MAXIMO,COD_REGISTRO)
        VALUES (?,?,?,?);
'''), (id_reg,ean1,preco_mc_20pc,registro)

conn.executescript('''
    INSERT INTO LABORATORIO(ID_LAB,NOME_LAB,CNPJ,ID_REG) 
        VALUES (?,?,?,?);
'''), (id_lab,laboratorio,cnpj,id_reg)

conn.executescript('''
    INSERT INTO SUBSTANCIA(ID_SUBSTANCIA,NOME_SUBS) 
        VALUES (?,?);
'''), (id_substancia,substancia)

conn.executescript('''
    INSERT INTO APRESENTACAO(ID_APRESENTACAO,DESCRICAO, COD_GGREM) 
        VALUES (?,?,?);
'''), (id_apresentacao,descricao,codigo_ggrem)

conn.executescript('''
    INSERT INTO TIPO(ID_TIPO,STATUS) 
        VALUES (?,?);
'''), (id_tipo,status)

conn.commit()
