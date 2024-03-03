import psycopg2

def printar(resultados):
    for linha in resultados:
        print(linha)


conn = psycopg2.connect(host="200.129.44.249", dbname="538704", user="538704", password="538704@fbd")

cur = conn.cursor()

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
EXCEPTION WHEN others THEN
    -- Tratamento da exceção
    RETURN NULL;
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
    -- Verifica se a movimentação está associada a um empregado que não é da manutenção
    IF EXISTS (
        SELECT 1
        FROM Empregados E
        WHERE E.id_emp = NEW.id_emp AND E.funcao <> 'Manutencao'
    ) THEN
        -- Se o empregado não tiver a função 'Manutencao', gera uma exceção
        RAISE EXCEPTION 'Somente empregados da manutenção podem estar associados a movimentações';
    END IF;
    RETURN NEW;
EXCEPTION WHEN others THEN
    -- Tratamento da exceção
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
            
CREATE TRIGGER trigger_restricao_empregado_manutencao
BEFORE INSERT OR UPDATE OF tipo ON Movimentacao
FOR EACH ROW
EXECUTE FUNCTION restricao_empregado_manutencao();
            
CREATE TRIGGER trigger_restricao_empregado_manutencao_movimentacao_empregados
BEFORE INSERT OR UPDATE OF id_mov, id_emp ON Movimentacao_Empregados
FOR EACH ROW
EXECUTE FUNCTION restricao_empregado_manutencao();
""")

conn.commit()

cur.close()
conn.close()