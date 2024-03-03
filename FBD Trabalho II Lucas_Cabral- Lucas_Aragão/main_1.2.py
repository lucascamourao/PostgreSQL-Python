import psycopg2

def printar(resultados):
    for linha in resultados:
        print(linha)

conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")
#trabalho_1_Lucas^2
# Em dbname, escolha o banco de dados
# Colocar o usuário e senha matricula@fbd
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
#quantidade de movimentacoes do tipo cargueiro
cur.execute(""" SELECT COUNT(*) as quantidade_cargueiro
            FROM Movimentacao M, Movimentacao_Empregados Me
            Where M.id_mov = Me.id_mov AND M.tipo = 'Carga'
            GROUP BY M.tipo
""")

res3 = cur.fetchall()
print("\nRetorne a quantidade de movimentações que envolvem embarcações do tipo Cargueiro.\n")
printar(res3)

# ===============================================================================================================
# 2.5 fazer a transacao em resumo criar uma funcao que insira 2 valores novos e faça um select(igual ao item c da 2.4)
cur.execute("""DROP FUNCTION transacao() CASCADE""")

cur.execute( """CREATE OR REPLACE FUNCTION transacao()
                RETURNS TABLE (quantidade_cargueiro bigint) as $$
                BEGIN
                    INSERT INTO Movimentacao(id_mov, data, tipo, id_emb) VALUES
                    (6, '2023-10-05'::DATE, 'Manutencao', 1);

                    INSERT INTO Movimentacao_Empregados(id_mov, id_emp) VALUES
                    (6, 1);
                    
                    RETURN QUERY SELECT COUNT(*) as quantidade_cargueiro
                    FROM Movimentacao M, Movimentacao_Empregados Me
                    WHERE M.id_mov = Me.id_mov AND M.tipo = 'Carga'
                    GROUP BY M.tipo;
                END
                $$ LANGUAGE plpgsql;
""")

#executar funcao

cur.execute(""" SELECT * FROM transacao()""")

fun1 = cur.fetchall()
print('\nResultados 2.5')
print(fun1)
printar(fun1)



# 2.6 
# funcao que pega o funcionario que tem mais movimentacoes naquele mes daquele ano especifico
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

# usando de exemplo a data 2023- 10-01

cur.execute( """ SELECT * FROM empregado_do_mes('2023-10-01'::DATE)""")
funcionario_do_mes = cur.fetchall()
print('\nFuncionario do mes:')
printar(funcionario_do_mes)

# 2.7 - Triggers

# 1. Implemente um gatilho no banco de dados que dispara toda vez que um Tripulante é cadastrado ou quando o atributo “funcao” tem 
# seu valor modificado. O gatilho deve garantir que somente um dos tripulantes tenha a função “Capitão”.
cur.execute("""
CREATE OR REPLACE FUNCTION verificar_funcao_capitao()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.funcao = 'Capitao' THEN
        IF EXISTS (
            SELECT 1
            FROM Tripulantes
            WHERE id_emb = NEW.id_emb AND funcao = 'Capitao' AND id_trp <> NEW.id_trp
        ) THEN
            RAISE EXCEPTION 'Já existe um capitão para esta embarcação';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_verificar_funcao_capitao
BEFORE INSERT OR UPDATE OF funcao ON Tripulantes
FOR EACH ROW
EXECUTE FUNCTION verificar_funcao_capitao();
""")

# 2. Crie um segundo gatilho que restrinja que somente empregados da manutenção possam ser escolhidos para executar movimentaçõs do tipo “Manutencao”.
cur.execute("""
CREATE OR REPLACE FUNCTION restricao_empregado_manutencao()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.tipo = 'Manutencao' THEN
        PERFORM 1
        FROM Empregados E
        WHERE E.id_emp = NEW.id_emp AND E.funcao = 'Manutencao';
        
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Somente empregados da manutenção podem executar movimentações do tipo "Manutencao"';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
            
CREATE TRIGGER trigger_restricao_empregado_manutencao
BEFORE INSERT OR UPDATE OF tipo ON Movimentacao
FOR EACH ROW
EXECUTE FUNCTION restricao_empregado_manutencao();
""")

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
conn.close()