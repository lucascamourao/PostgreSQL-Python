import psycopg2

def printar(resultados):
    for linha in resultados:
        print(linha)


conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

# 3. Por fim escreva um script em python que tenta inserir as tuplas como descrito na Tabela 6
cur.execute("""INSERT INTO Movimentacao_Empregados(id_mov, id_emp) VALUES
(5, 5),
(5, 2)
""")

# 4. Adicione ao script uma linha que tenta modificar o valor do atributo “funcao” do Tripulante3 para “Capitão” segundo a Tabela 7.

# Adicionar primeiro na tabela
cur.execute("""INSERT INTO Tripulantes(id_trp, nome, data_nasc, funcao, id_emb) VALUES
(7, 'Tripulante7', '1980-09-04', 'Capitao', 4),
(8, 'Tripulante8', '1985-03-03', 'Capitao', 2)
""")

cur.execute("""
UPDATE Tripulantes
SET funcao = 'Capitao'
WHERE id_trp = 3;   
""")

conn.commit()

cur.close()