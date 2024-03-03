import psycopg2

def printar(resultados):
    for linha in resultados:
        print(linha)

conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

cur.execute("""DROP FUNCTION IF EXISTS empregado_do_mes(data_esp DATE) CASCADE""")

cur.execute(""" CREATE OR REPLACE FUNCTION empregado_do_mes(data_esp DATE)
                RETURNS TABLE(id_emp int, nome varchar) AS $$
                BEGIN 
                    RETURN QUERY SELECT  E.id_emp, E.nome
                    FROM Empregados E, Movimentacao M, Movimentacao_Empregados Me
                    WHERE M.id_mov = Me.id_mov AND E.id_emp = Me.id_emp AND DATE_PART('month', M.data) = DATE_PART('month', data_esp) AND DATE_PART('year',M.DATA) = DATE_PART('year',data_esp)
                    GROUP BY E.id_emp
                    HAVING COUNT(*) = (SELECT MAX(qntd)
                                       FROM (SELECT E.id_emp,COUNT(*) as qntd
                                       FROM Empregados E, Movimentacao M, Movimentacao_Empregados Me
                                       WHERE M.id_mov = Me.id_mov AND E.id_emp = Me.id_emp AND DATE_PART('month', M.data) = DATE_PART('month', data_esp) AND DATE_PART('year',M.DATA) = DATE_PART('year',data_esp)
                                       GROUP BY E.id_emp) AS sub);
                END
                $$ LANGUAGE plpgsql;
""")

# usando de exemplo a data 2023-10-01

cur.execute( """ SELECT * FROM empregado_do_mes('2023-10-01'::DATE)""")
funcionario_do_mes = cur.fetchall()
print('\nFuncionario do mes:')
printar(funcionario_do_mes)

conn.commit()

cur.close()
conn.close()