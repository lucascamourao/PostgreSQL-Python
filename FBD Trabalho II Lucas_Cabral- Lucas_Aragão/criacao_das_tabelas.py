import psycopg2

conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

#dropando todas as tabelas 
tabelas = ['Embarcacoes','Tripulantes','Empregados','Movimentacao', 'Movimentacao_empregados']
for tabela in tabelas:
    cur.execute(f"""DROP TABLE IF EXISTS {tabela} CASCADE """)

#Tabela Embarcacoes
cur.execute("""CREATE TABLE IF NOT EXISTS Embarcacoes(
    id_emb INT PRIMARY KEY,
    nome VARCHAR(30),
    tipo VARCHAR(30)
)
""")

#Tabela Tripulantes
cur.execute("""CREATE TABLE IF NOT EXISTS Tripulantes(
    id_trp INT PRIMARY KEY,
    nome VARCHAR(30),
    data_nasc DATE, 
    funcao VARCHAR(30),
    id_emb INT,
    FOREIGN KEY (id_emb) REFERENCES Embarcacoes(id_emb)
)
""")

#Tabela Empregados
cur.execute("""CREATE TABLE IF NOT EXISTS Empregados(
    id_emp INT PRIMARY KEY,
    nome VARCHAR(30),
    data_nasc DATE, 
    funcao VARCHAR(30)
)
""")

#Tabela Movimentacao
cur.execute("""CREATE TABLE IF NOT EXISTS Movimentacao(
    id_mov INT PRIMARY KEY,
    data DATE, 
    tipo VARCHAR(30),
    id_emb INT, 
    FOREIGN KEY (id_emb) REFERENCES Embarcacoes(id_emb)
)
""")

#Tabela Movimentacao_Empregados
cur.execute("""CREATE TABLE IF NOT EXISTS Movimentacao_Empregados(
    id_mov INT,
    id_emp INT, 
    FOREIGN KEY (id_mov) REFERENCES Movimentacao(id_mov),
    FOREIGN KEY (id_emp) REFERENCES Empregados(id_emp)
)
""")

conn.commit()

cur.close()
conn.close()
print('deu certo')