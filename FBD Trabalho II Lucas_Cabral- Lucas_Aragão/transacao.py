import psycopg2

def printar(resultados):
    for linha in resultados:
        print(linha)


conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

# ===============================================================================================================
# 2.5 fazer a transacao em resumo criar uma funcao que insira 2 valores novos e faça um select(igual ao item c da 2.4)
cur.execute("""DROP FUNCTION IF EXISTS transacao() CASCADE""")

cur.execute( """CREATE OR REPLACE FUNCTION transacao()
                RETURNS TABLE (quantidade_cargueiro bigint) as $$
                BEGIN
                    INSERT INTO Movimentacao(id_mov, data, tipo, id_emb) VALUES
                    (6, '2023-10-05'::DATE, 'Manutencao', 1);

                    INSERT INTO Movimentacao_Empregados(id_mov, id_emp) VALUES
                    (6, 1);
                    
                    RETURN QUERY SELECT COUNT(*) as quantidade_cargueiro
                    FROM Movimentacao M, Embarcacoes E
                    WHERE E.tipo = 'Cargueiro' AND M.id_emb = E.id_emb
                    GROUP BY E.tipo;
                END
                $$ LANGUAGE plpgsql;
""")

#executar funcao

cur.execute(""" SELECT * FROM transacao()""")

fun1 = cur.fetchall()
print('\nResultados 2.5')
printar(fun1)

conn.commit()

cur.close()
conn.close()