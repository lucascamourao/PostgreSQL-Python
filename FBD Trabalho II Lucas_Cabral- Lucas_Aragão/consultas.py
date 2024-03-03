import psycopg2

def printar(resultados):
    for linha in resultados:
        print(linha)

conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

print("\nConsultas SQL 2.4") # ========================================================================

#embarcacoes e numero de tripulantes de cada uma
cur.execute(""" SELECT E.*, Count(*) as num_tripulantes
            FROM Embarcacoes E, Tripulantes T
            WHERE E.id_emb = T.id_emb
            GROUP BY E.id_emb
""")

res1 = cur.fetchall()
print("\nRetorne todas as embarcações e o numero de tripulantes de cada embarção.\n")
printar(res1)

#empregados envolvidos na movimentacao id_mov = 1
cur.execute("""SELECT E.* 
            FROM Empregados E, Movimentacao_Empregados Me
            WHERE E.id_emp =  Me.id_emp AND id_mov = 1 
""")

res2 = cur.fetchall()
print("\nRetorne os Empregados envolvidos na movimentação de ID 1.\n")
printar(res2)
#quantidade de movimentacoes em embarcacoes do tipo cargueiro

cur.execute(""" SELECT COUNT(*) as quantidade_cargueiro
                FROM Movimentacao M, Embarcacoes E
                WHERE E.tipo = 'Cargueiro' AND M.id_emb = E.id_emb
                GROUP BY E.tipo;
""")

res3 = cur.fetchall()
print("\nRetorne a quantidade de movimentações que envolvem embarcações do tipo Cargueiro.\n")
printar(res3)

conn.commit()

cur.close()
conn.close()