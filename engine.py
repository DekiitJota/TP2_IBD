import io
import sqlite3
import csv
import pandas as pd

conn = sqlite3.connect('/tmp/preco_medicamentos.sql')
cur = conn.cursor()

conn.executescript('''
    PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE PRODUTO (
        ID_PRODUTO INTEGER PRIMARY KEY, 
        NOME TEXT,
        CLASSE TEXT
        ID_REG INTEGER REFERENCES REGISTRO (ID_REG)
    );
    CREATE TABLE REGISTRO (
        ID_REG INTEGER PRIMARY KEY,
        EAN1 VARCHAR,
        PRECO_MAXIMO NUMERIC,
        COD_REGISTRO INTEGER
    );
    CREATE TABLE LABORATORIO (
        ID_LAB INTEGER PRIMARY KEY,
        NOME_LAB VARCHAR,
        CNPJ VARCHAR,
        ID_REG INTEGER REFERENCES REGISTRO (ID_REG)
    );
    CREATE TABLE SUBSTANCIA (
        ID_SUBSTANCIA TEXT,
        NOME TEXT			
    ); 
    CREATE TABLE APRESENTACAO (
        ID_APRESENTACAO INTEGER PRIMARY KEY,
        DESCRICAO TEXT,
        COD_GGREM INTEGER
    );
    CREATE TABLE APRESENTACAO (
        ID_TIPO INTEGER,
        NOME TEXT,
        PRIMARY KEY(ID_TIPO)
    );
''')

with open('/tmp/TA_PRECO_MEDICAMENTO.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        _id=row[0]
        dataNotificacao=row[1]
        cnes=row[2]
        ocupacaoSuspeitoCli=float(row[3])
        ocupacaoSuspeitoUti=float(row[4])
        ocupacaoConfirmadoCli=float(row[5])
        ocupacaoConfirmadoUti=float(row[6])
        saidaSuspeitaObitos=float(row[7])
        saidaSuspeitaAltas=float(row[8])
        saidaConfirmadaObitos=float(row[9])
        saidaConfirmadaAltas=float(row[10])
        origem=row[11]
        _p_usuario=row[12]
        estadoNotificacao=row[13]
        municipioNotificacao=row[14]
        estado=row[15]
        municipio=row[16]
        excluido=row[17]
        validado=row[18]
        _created_at=row[19]
        _updated_at=row[20]
        
        cur.executescript('''
        INSERT INTO registro_ocupacao(_id,dataNotificacao,cnes,ocupacaoSuspeitoCli,ocupacaoSuspeitoUti,ocupacaoConfirmadoCli,ocupacaoConfirmadoUti,saidaSuspeitaObitos,saidaSuspeitaAltas,saidaConfirmadaObitos,saidaConfirmadaAltas,origem,_p_usuario,estadoNotificacao,municipioNotificacao,estado,municipio,excluido,validado,_created_at,_updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        '''), (_id,dataNotificacao,cnes,ocupacaoSuspeitoCli,ocupacaoSuspeitoUti,ocupacaoConfirmadoCli,ocupacaoConfirmadoUti,saidaSuspeitaObitos,saidaSuspeitaAltas,saidaConfirmadaObitos,saidaConfirmadaAltas,origem,_p_usuario,estadoNotificacao,municipioNotificacao,estado,municipio,excluido,validado,_created_at,_updated_at)
        conn.commit()