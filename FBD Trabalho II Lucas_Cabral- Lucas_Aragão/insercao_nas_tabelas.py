import psycopg2

conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

#Adicionando na tabela Embarcacoes
cur.execute("""INSERT INTO Embarcacoes(id_emb, nome, tipo) VALUES
(1, 'Navio1', 'Cargueiro'),
(2, 'Navio2', 'Passageiro'),
(3, 'Navio3', 'Petroleiro'),
(4, 'Navio4', 'Cargueiro')
""")

cur.execute("""INSERT INTO Tripulantes(id_trp, nome, data_nasc, funcao, id_emb) VALUES
(1, 'Tripulante1', '1990-01-15', 'Oficial de Conves', 1),
(2, 'Tripulante2', '1992-03-20', 'Engenheiro', 1),
(3, 'Tripulante3', '1988-11-05', 'Comissario de bordo', 2),
(4, 'Tripulante4', '1995-06-30', 'Oficial de Conves', 3),
(5, 'Tripulante5', '1991-07-10', 'Capitao', 4),
(6, 'Tripulante6', '1994-09-25', 'Engenheiro', 4)
""")

cur.execute("""INSERT INTO Empregados(id_emp, nome, data_nasc, funcao) VALUES
(1, 'Employee1', '1985-05-12', 'Manutencao'),
(2, 'Employee2', '1993-02-28', 'Segurança'),
(3, 'Employee3', '1987-09-18', 'Logistica'),
(4, 'Employee4', '1990-12-05', 'Limpeza'),
(5, 'Employee5', '2001-08-30', 'Manutencao')
""")

cur.execute("""INSERT INTO Movimentacao(id_mov, data, tipo, id_emb) VALUES
(1, '2023-09-01', 'Carga', 1),
(2, '2023-09-02', 'Embarque de passageiros', 2),
(3, '2023-10-03', 'Abastecimento', 3),
(4, '2023-10-05', 'Descarga', 1),
(5, '2023-10-05', 'Manutencao', 4)
""")

cur.execute("""INSERT INTO Movimentacao_Empregados(id_mov, id_emp) VALUES
(1, 1),
(1, 3),
(2, 2),
(3, 1),
(3, 4),
(4, 1),
(4, 3),
(5, 1)
""")

conn.commit()

cur.close()
conn.close()
print('deu certo')