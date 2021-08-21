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
        EAN1 VARCHAR,
        PRECO_MAXIMO NUMERIC,
        COD_REGISTRO INTEGER,
        ID_LAB REFERENCE LABORATORIO (ID_LABORATORIO)
    );
    CREATE TABLE IF NOT EXISTS LABORATORIO (
        ID_LAB INTEGER PRIMARY KEY,
        NOME_LAB VARCHAR,
        CNPJ VARCHAR,
        ID_REG INTEGER REFERENCES REGISTRO (ID_REG)
    );
    CREATE TABLE IF NOT EXISTS SUBSTANCIA (
        ID_SUBSTANCIA TEXT,
        NOME TEXT			
    ); 
    CREATE TABLE IF NOT EXISTS APRESENTACAO (
        ID_APRESENTACAO INTEGER PRIMARY KEY,
        DESCRICAO TEXT,
        COD_GGREM INTEGER
    );
    CREATE TABLE IF NOT EXISTS TIPO (
        ID_TIPO INTEGER PRIMARY KEY,
        NOME TEXT
    );
    COMMIT;
''')

with open('/tmp/TA_PRECO_MEDICAMENTO.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        substancia=row[0]
        cnpj=row[1]
        codigo_ggrem=row[2]
        registro=row[3]
        ean1=row[4]
        ean2=row[5]
        ean2=row[6]
        produto_apresentacao=row[7]
        classe=row[8]
        tipo=row[9]
        regime_preco=row[10]
        origem=row[11]
        preco_fab=row[12] #preco de fabrica s/ impostos
        #preço de fabrica @ x% = Xpc
        preco_fab_0pc=row[13] 
        preco_fab_12pc=row[14]
        preco_fab_17pc=row[15]
        preco_fab_17pc_alc=row[16]
        preco_fab_17_5pc=row[17]
        preco_fab_17_5pc_alc=row[18]
        preco_fab_18pc=row[19]
        preco_fab_18pc_alc=row[20]
        preco_fab_20pc=row[21]
        #preço de mercado @ x% = Xpc
        preco_mc_0pc=row[22]
        preco_mc_12pc=row[23]
        preco_mc_17pc=row[24]
        preco_mc_17pc_alc=row[25]
        preco_mc_17_5pc=row[26]
        preco_mc_17_5pc_alc=row[27]
        preco_mc_18pc=row[28]
        preco_mc_18pc_alc=row[29]
        preco_mc_20pc=row[30]
        restricao_hosp=row[31]
        cap=row[32]
        confaz=row[33]
        icms=row[34]
        analise_recursal=row[35]
        cr_tributario=row[36]
        comercializacao=row[37]
        tarja=row[38]

cur.executescript('''
    INSERT INTO registro_ocupacao(_id,dataNotificacao,cnes,ocupacaoSuspeitoCli,ocupacaoSuspeitoUti,ocupacaoConfirmadoCli,ocupacaoConfirmadoUti,saidaSuspeitaObitos,saidaSuspeitaAltas,saidaConfirmadaObitos,saidaConfirmadaAltas,origem,_p_usuario,estadoNotificacao,municipioNotificacao,estado,municipio,excluido,validado,_created_at,_updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''), (_id,dataNotificacao,cnes,ocupacaoSuspeitoCli,ocupacaoSuspeitoUti,ocupacaoConfirmadoCli,ocupacaoConfirmadoUti,saidaSuspeitaObitos,saidaSuspeitaAltas,saidaConfirmadaObitos,saidaConfirmadaAltas,origem,_p_usuario,estadoNotificacao,municipioNotificacao,estado,municipio,excluido,validado,_created_at,_updated_at)

conn.commit()