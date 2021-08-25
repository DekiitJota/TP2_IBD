import sqlite3
import csv
import pandas as pd

conn = sqlite3.connect('/tmp/preco_medicamentos.sql')
cur = conn.cursor()

cur.executescript("""
    PRAGMA foreign_keys=ON;
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

class DataID:
    def __init__(self):
        self.data = {}
        self.last_indx = 0
    def add(self, item):
        if (item not in self.data):
            self.data[item] = self.last_indx
            self.last_indx += 1
    def has(self, item):
        return item in self.data
    def get_index(self, item):
        return self.data[item]

produtos = DataID()
apresentacoes = DataID()
substancias = DataID()
tipos = DataID()
registros = DataID()
laboratorios = DataID()

produto_tipo = set()

with open('/tmp/TA_PRECO_MEDICAMENTO.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    # Pula a primeira linha
    next(csv_reader) 
    # Contador que limita as linhas lidas do arquivo
    counter = 0

    for row in csv_reader:
      # Lida com a limitação no numero de linhas
      if counter >= MAX_ROWS:
        break
      counter += 1

      # Extrai os dados da linha
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
      nome_tipo=row[11]
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

      if not substancias.has(substancia):
        substancias.add(substancia)
        with conn:
            cur.execute("INSERT INTO SUBSTANCIA(ID_SUBSTANCIA,NOME_SUBS) VALUES (?,?)", (substancias.get_index(substancia), substancia))

      if not tipos.has(nome_tipo):
        tipos.add(nome_tipo)
        with conn:
            cur.execute("INSERT INTO TIPO(ID_TIPO,NOME_TIPO) VALUES (?,?)", (tipos.get_index(nome_tipo), nome_tipo,))

      if not produtos.has(produto):
        produtos.add(produto)
        with conn:
            cur.execute("INSERT INTO PRODUTO(ID_PRODUTO,NOME,CLASSE,ID_SUBSTANCIA,ID_TIPO) VALUES (?,?,?,?,?)", (produtos.get_index(produto),produto,classe,substancias.get_index(substancia),tipos.get_index(nome_tipo)))
      
      if not laboratorios.has(cnpj):
        laboratorios.add(cnpj)
        with conn:
            cur.execute("INSERT INTO LABORATORIO(ID_LAB,NOME_LAB,CNPJ) VALUES (?,?,?)", (laboratorios.get_index(cnpj),laboratorio,cnpj))

      if not registros.has(ean1):
        registros.add(ean1)
        with conn:
            cur.execute("INSERT INTO REGISTRO(ID_REG, EAN1,TARJA,PRECO_MAXIMO,COD_REGISTRO,ID_LAB,ID_PRODUTO) VALUES (?,?,?,?,?,?,?)", (registros.get_index(ean1),ean1,tarja,preco_mc_20pc,registro,produtos.get_index(produto),laboratorios.get_index(cnpj)))

      if not apresentacoes.has(codigo_ggrem):
        apresentacoes.add(codigo_ggrem)
        with conn:
            cur.execute("INSERT INTO APRESENTACAO(ID_APRESENTACAO,DESCRICAO,COD_GGREM,ID_PRODUTO)VALUES (?,?,?,?)", (apresentacoes.get_index(codigo_ggrem),descricao,codigo_ggrem,produtos.get_index(produto)))

      if (produto, nome_tipo) not in produto_tipo:
        produto_tipo.add((produto, nome_tipo))
        with conn:
            cur.execute("INSERT INTO PRODUTO_TIPO(ID_PRODUTO,ID_TIPO)VALUES (?,?)", (produtos.get_index(produto),tipos.get_index(nome_tipo)))

      
conn.commit()