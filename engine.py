import sqlite3
import csv
import pandas as pd

conn = sqlite3.connect('/tmp/preco_medicamentos.sql')
cur = conn.cursor()

cur.executescript("""
    PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS PRODUTO (
        ID_PRODUTO INTEGER PRIMARY KEY, 
        NOME TEXT,
        CLASSE TEXT,
        ID_SUBSTANCIA INTEGER,
        ID_TIPO INTEGER,
          FOREIGN KEY (ID_SUBSTANCIA) REFERENCES SUBSTANCIA (ID_SUBSTANCIA),
          FOREIGN KEY (ID_TIPO) REFERENCES TIPO (ID_TIPO)
    );
    CREATE TABLE IF NOT EXISTS REGISTRO (
        ID_REG INTEGER PRIMARY KEY,
        EAN1 TEXT,
        TARJA TEXT,
        PRECO_MAXIMO NUMERIC,
        COD_REGISTRO INTEGER,
        ID_LAB INTEGER,
        ID_PRODUTO INTEGER,
          FOREIGN KEY (ID_PRODUTO) REFERENCES PRODUTO (ID_PRODUTO),
          FOREIGN KEY (ID_LAB) REFERENCES LABORATORIO (ID_LAB)
    );
    CREATE TABLE IF NOT EXISTS LABORATORIO (
        ID_LAB INTEGER PRIMARY KEY,
        NOME_LAB TEXT,
        CNPJ TEXT
    );
    CREATE TABLE IF NOT EXISTS SUBSTANCIA (
        ID_SUBSTANCIA INTEGER PRIMARY KEY,
        NOME_SUBS TEXT			
    ); 
    CREATE TABLE IF NOT EXISTS APRESENTACAO (
        ID_APRESENTACAO INTEGER PRIMARY KEY,
        DESCRICAO TEXT,
        COD_GGREM INTEGER,
        ID_PRODUTO INTEGER,
          FOREIGN KEY (ID_PRODUTO) REFERENCES PRODUTO (ID_PRODUTO)
    );
    CREATE TABLE IF NOT EXISTS TIPO (
        ID_TIPO INTEGER PRIMARY KEY,
        STATUS TEXT
    );
    COMMIT;
""")

with open('/tmp/test2.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        #print(row) #remove
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
        with conn:
            cur.execute("INSERT INTO PRODUTO(NOME,CLASSE) VALUES (?,?)", (produto,classe))
            cur.execute("INSERT INTO REGISTRO(EAN1,TARJA,PRECO_MAXIMO,COD_REGISTRO) VALUES (?,?,?,?)", (ean1,tarja,preco_mc_20pc,registro))
            cur.execute("INSERT INTO LABORATORIO(NOME_LAB,CNPJ) VALUES (?,?)", (laboratorio,cnpj))
            cur.execute("INSERT INTO SUBSTANCIA(NOME_SUBS) VALUES (?)", (substancia,))
            cur.execute("INSERT INTO APRESENTACAO(DESCRICAO,COD_GGREM)VALUES (?,?)", (descricao,codigo_ggrem))
            cur.execute("INSERT INTO TIPO(STATUS) VALUES (?)", (status,))
        
        #deleta o header do arquivo csv:
        cur.execute("DELETE FROM PRODUTO WHERE (NOME == 'PRODUTO')")
        cur.execute("DELETE FROM REGISTRO WHERE (COD_REGISTRO == 'REGISTRO')")
        cur.execute("DELETE FROM LABORATORIO WHERE (NOME_LAB == 'LABORATORIO')")
        cur.execute("DELETE FROM SUBSTANCIA WHERE (NOME_SUBS == 'SUBSTANCIA')")
        cur.execute("DELETE FROM APRESENTACAO WHERE (DESCRICAO == 'APRESENTACAO')")
        cur.execute("DELETE FROM TIPO WHERE (STATUS == 'TIPO DE PRODUTO (STATUS DO PRODUTO)')")
conn.commit()
