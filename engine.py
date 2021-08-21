import io
import sqlite3
import csv
import pandas as pd

conn = sqlite3.connect('/tmp/preco_medicamentos.sql')
cur = conn.cursor()

conn.executescript('''
    PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS PRODUTO (
        ID_PRODUTO INTEGER PRIMARY KEY, 
        NOME TEXT,
        CLASSE TEXT
        ID_REG INTEGER REFERENCES REGISTRO (ID_REG),
        ID_SUBSTANCIA INTEGER REFERENCES SUBSTANCIA (ID_SUBSTANCIA),
        ID_APRESENTACAO INTEGER REFERENCES APRESENTACAO (ID_APRESENTACAO),
        ID_TIPO INTEGER REFERENCES TIPO (ID_TIPO)
    );
    CREATE TABLE IF NOT EXISTS REGISTRO (
        ID_REG INTEGER PRIMARY KEY,
        EAN1 TEXT,
        PRECO_MAXIMO NUMERIC,
        COD_REGISTRO INTEGER,
        ID_LABORATORIO REFERENCE LABORATORIO (ID_LABORATORIO)
    );
    CREATE TABLE IF NOT EXISTS LABORATORIO (
        ID_LABORATORIO INTEGER PRIMARY KEY,
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
        NOME_TIPO TEXT
    );
    COMMIT;
''')

with open('/tmp/TA_PRECO_MEDICAMENTO.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        substancia=row[0]
        cnpj=row[1]
        laboratorio=row[]
        codigo_ggrem=row[2]
        registro=row[3]
        ean1=row[4]
        ean2=row[5]
        ean3=row[6]
        produto=row[7]
        descricao=row[8]
        classe=row[9]
        tipo=row[10]
        regime_preco=row[11]
        origem=row[12]
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

#TO DO: Implementar identadores para SUBSTANCIA, REGISTRO, LABORATORIO, SUBSTANCIA, APRESENTACAO, TIPO

cur.executescript('''
    INSERT INTO PRODUTO(ID_PRODUTO,NOME,CLASSE,ID_REG,ID_SUBSTANCIA,ID_APRESENTACAO,ID_TIPO)
        VALUES (?,?,?,?,?,?,?)
'''), (id_produto,produto,classe,id_reg,id_substancia,id_apresentacao,id_tipo)

#TO DO: rever "PRECO_MAXIMO"
cur.executescript('''
    INSERT INTO REGISTRO(ID_REG,EAN1,PRECO_MAXIMO,REGISTRO,ID_LABORATORIO) 
        VALUES (?,?,?,?,?)
'''), (id_reg,ean1,preco_mc_20pc,registro,id_lab)

cur.executescript('''
    INSERT INTO LABORATORIO(ID_LABORATORIO,NOME_LAB,CNPJ,ID_REG) 
        VALUES (?,?,?,?)
'''), (id_lab,laboratorio,cnpj,id_reg)

cur.executescript('''
    INSERT INTO SUBSTANCIA(ID_SUBSTANCIA,NOME_SUBS) 
        VALUES (?,?)
'''), (id_substancia,substancia)

cur.executescript('''
    INSERT INTO APRESENTACAO(ID_APRESENTACAO,DESCRICAO, COD_GGREM) 
        VALUES (?,?,?)
'''), (id_apresentacao,descricao,codigo_ggrem)

#TIPO = TARJA?
cur.executescript('''
    INSERT INTO TARJA(ID_TARJA,NOME_TARJA) 
        VALUES (?,?)
'''), (id_tarja,tarja)

conn.commit()