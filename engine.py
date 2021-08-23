import sqlite3
import csv
import pandas as pd

conn = sqlite3.connect('/tmp/preco_medicamentos.sql')
cur = conn.cursor()

conn.executescript('''
    PRAGMA foreign_keys=ON;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS PRODUTO (
        ID_PRODUTO INTEGER PRIMARY KEY, 
        NOME TEXT,
        CLASSE TEXT,
        TARJA TEXT,
        ID_REG INTEGER,
          FOREIGN KEY (ID_REG) REFERENCES REGISTRO (ID_REG),
        ID_SUBSTANCIA INTEGER,
          FOREIGN KEY (ID_SUBSTANCIA) REFERENCES SUBSTANCIA (ID_SUBSTANCIA),
        ID_APRESENTACAO INTEGER,
          FOREIGN KEY (ID_APRESENTACAO) REFERENCES APRESENTACAO (ID_APRESENTACAO),
        ID_TIPO INTEGER,
          FOREIGN KEY (ID_TIPO) REFERENCES TIPO (ID_TIPO)
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
        ID_REG INTEGER,
          FOREIGN KEY (ID_REG) REFERENCES REGISTRO (ID_REG)
    );
    CREATE TABLE IF NOT EXISTS SUBSTANCIA (
        ID_SUBSTANCIA INTEGER PRIMARY KEY,
        NOME_SUBS TEXT			
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

with open('/tmp/TA_PRECO_MEDICAMENTO.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        print(row) #remove
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

def insert_produto(produto):
    with conn:
        conn.execute("INSERT INTO PRODUTO(NOME,CLASSE,TARJA) VALUES (?,?,?)", (produto,classe,tarja))

def insert_registro(registro):
    with conn:
        conn.execute("INSERT INTO REGISTRO(EAN1,PRECO_MAXIMO,COD_REGISTRO) VALUES (?,?,?)", (ean1,preco_mc_20pc,registro))

def insert_laboratorio(laboratorio):
    with conn:
        conn.execute("INSERT INTO LABORATORIO(NOME_LAB,CNPJ) VALUES (?,?)", (laboratorio,cnpj))

def insert_substancia(substancia):
    with conn:
        conn.execute("INSERT INTO SUBSTANCIA(NOME_SUBS) VALUES (?)", (substancia,))

def insert_apresentacao(apresentacao):
    with conn:
        conn.execute("INSERT INTO APRESENTACAO(DESCRICAO, COD_GGREM)VALUES (?,?)", (descricao,codigo_ggrem))

def insert_tipo(tipo):
    with conn:
        conn.execute("INSERT INTO TIPO(STATUS) VALUES (?)", (status,))

conn.commit()