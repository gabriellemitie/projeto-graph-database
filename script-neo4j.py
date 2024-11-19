# para visualizar os dados em formato de nós, abrir neo4j browser que tem no neo4j desktop -> qqr duvida chamar gabi
# link para introdução a cypher -> https://graphacademy.neo4j.com/courses/cypher-fundamentals/1-reading/1-intro-cypher/
# instalar o neo4j desktop 

import psycopg 
from neo4j import GraphDatabase
from decimal import Decimal


# Conectando ao PostgreSQL
def connect_postgres():
    try:
        conn = psycopg.connect('postgresql://postgres:senha@localhost:5432/bancodedados') # inserir senha e banco de dados
        print("Conexão com PostgreSQL estabelecida com sucesso!")
        return conn
    except Exception as e:
        print(f"Erro na conexão com PostgreSQL: {e}")
        return None

# Conectando ao Neo4j
def connect_neo4j():
    try:
        uri = "bolt://localhost:7687"  # Bolt URL padrão do Neo4j
        driver = GraphDatabase.driver(uri, auth=("neo4j", "12345678"))  # Autenticação (usuário, senha) no caso usuario padrao é o neo4j, senha vc coloca a que vc criar quando criar um noov banco no neo4j
        print("Conectado ao Neo4j")
        return driver
    except Exception as e:
        print(f"Erro ao conectar ao Neo4j: {e}")
        return None

# Função para extrair dados de uma tabela PostgreSQL
def fetch_data_from_postgres(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
    return results

# Função genérica para extrair dados de qualquer tabela
def fetch_table_data(conn, table, columns):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(columns)} FROM {table}")
        rows = cur.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
    return results

# Função para listar todas as tabelas e colunas no PostgreSQL
def list_tables_and_columns(conn):
    query = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()
        tables = {}
        for table_name, column_name in result:
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(column_name)
    return tables

# ***************** CRIAÇÃO DAS TABELAS ************************************************
# OBSERVACOES -> MERGE USADO PARA CONSULTAR SE JA EXISTE OU NAO PARA EVITAR CRIACAO DE DADOS DUPLICADOS, MESMO VALE PARA O ON CREATE SET
def create_aluno_in_neo4j(driver, aluno_data):
    with driver.session() as session:
        query = """
        MERGE (a:Aluno {ra: $ra})
        ON CREATE SET a.nome_aluno = $nome_aluno, a.ano_matricula = $ano_matricula, a.id_tcc = $id_tcc
        """
        session.run(query, aluno_data)
        print(f"Aluno {aluno_data['nome_aluno']} inserido ou atualizado com sucesso.")

# Função para criar curso
def create_curso_in_neo4j(driver, curso_data):
    with driver.session() as session:
        query = """
        MERGE (cr:Curso {id_curso: $id_curso})
        ON CREATE SET cr.nome_curso = $nome_curso
        """
        session.run(query, curso_data)
        print(f"Curso {curso_data['nome_curso']} inserido ou atualizado com sucesso.")

def create_professor_departamento_in_neo4j(driver, professor_departamento_data):
    with driver.session() as session:
        query = """
        MERGE (p:Professor {codigo_prof: $codigo_prof})
        
        MERGE (dep:Departamento {nome_dep: $nome_dep})
        
        WITH p, dep
        MERGE (p)-[:PERTENCE_AO_DEPARTAMENTO]->(dep)
        """
        session.run(query, professor_departamento_data)
        print(f"Professor {professor_departamento_data['codigo_prof']} associado ao departamento {professor_departamento_data['nome_dep']} com sucesso.")

# Função para criar histec
def create_histesc_in_neo4j(driver, histec_data):
    # Verifica se a coluna nota é do tipo Decimal e converte para float
    if isinstance(histec_data.get('nota'), Decimal):
        histec_data['nota'] = float(histec_data['nota'])
        
    with driver.session() as session:
        query = """
        MERGE (h:Histec {nota: $nota, semestre_cursado: $semestre_cursado, ano_cursado: $ano_cursado})
        WITH h
        MATCH (a:Aluno {ra: $ra}), (d:Disciplina {codigo_disc: $codigo_disc})
        MERGE (h)-[:CURSOU]->(a)
        MERGE (h)-[:REFERENTE_A]->(d)
        """
        session.run(query, histec_data)
        print(f"Histec referente ao aluno {histec_data['ra']} e disciplina {histec_data['codigo_disc']} inserido ou atualizado com sucesso.")
# Função para criar histdisc
def create_histdisc_in_neo4j(driver, histdisc_data):
    with driver.session() as session:
        query = """
        MERGE (h:HistDisc {ano_ministrado: $ano_ministrado, semestre_ministrado: $semestre_ministrado})
        WITH h
        MATCH (p:Professor {codigo_prof: $codigo_prof}), (d:Disciplina {codigo_disc: $codigo_disc})
        MERGE (h)-[:MINISTRADA_POR]->(p)
        MERGE (h)-[:CORRESPONDE_A]->(d)
        """
        session.run(query, histdisc_data)
        print(f"HistDisc referente ao professor {histdisc_data['codigo_prof']} e disciplina {histdisc_data['codigo_disc']} inserido ou atualizado com sucesso.")

# Função para criar disciplina
def create_disciplina_in_neo4j(driver, disciplina_data):
    with driver.session() as session:
        query = """
        MERGE (d:Disciplina {codigo_disc: $codigo_disc})
        ON CREATE SET d.nome_disc = $nome_disc, d.ano_disc = $ano_disc, d.semestre_disc = $semestre_disc
        WITH d
        MATCH (p:Professor {codigo_prof: $codigo_prof}), (c:Curso {id_curso: $id_curso})
        MERGE (d)-[:MINISTRADA_POR]->(p)
        MERGE (d)-[:PERTENCE_AO_CURSO]->(c)
        """
        session.run(query, disciplina_data)
        print(f"Disciplina {disciplina_data['nome_disc']} inserida ou atualizada com sucesso.")

# Função para criar matriz curricular
def create_matrizcurricular_in_neo4j(driver, matriz_data):
    with driver.session() as session:
        query = """
        MERGE (m:MatrizCurricular {id_matriz: $id_matriz})
        ON CREATE SET m.semestre_aprovado = $semestre_aprovado, m.ano_aprovado = $ano_aprovado
        WITH m
        MATCH (c:Curso {id_curso: $id_curso}), (d:Disciplina {codigo_disc: $codigo_disc})
        MERGE (m)-[:PERTENCE_AO_CURSO]->(c)
        MERGE (m)-[:INCLUI_DISCIPLINA]->(d)
        """
        session.run(query, matriz_data)
        print(f"Matriz Curricular {matriz_data['id_matriz']} inserida ou atualizada com sucesso.")

# Função para criar formados
def create_formados_in_neo4j(driver, formados_data):
    with driver.session() as session:
        query = """
        MATCH (a:Aluno {ra: $ra}), (m:MatrizCurricular {id_matriz: $id_matriz})
        MERGE (a)-[:FORMADO_PELA]->(m)
        """
        session.run(query, formados_data)
        print(f"Aluno {formados_data['ra']} formado pela Matriz {formados_data['id_matriz']} inserido ou atualizado com sucesso.")

# Função para criar departamento
def create_departamento_in_neo4j(driver, departamento_data):
    with driver.session() as session:
        query = """
        MERGE (dep:Departamento {codigo_dep: $codigo_dep})
        ON CREATE SET dep.nome_dep = $nome_dep
        """
        session.run(query, departamento_data)
        print(f"Departamento {departamento_data['nome_dep']} inserido ou atualizado com sucesso.")

# Função para criar professor
def create_professor_in_neo4j(driver, professor_data):
    # Definir um valor de fallback para nome_prof
    if 'nome_prof' not in professor_data or not professor_data['nome_prof']:
        professor_data['nome_prof'] = 'Nome Desconhecido'

    with driver.session() as session:
        query = """
        MERGE (p:Professor {codigo_prof: $codigo_prof})
        ON CREATE SET p.nome_prof = $nome_prof, p.chefe_dep = $chefe_dep
        
        WITH p
        OPTIONAL MATCH (c:Curso {id_curso: $id_curso})
        OPTIONAL MATCH (t:Tcc {id_tcc: $id_tcc})
        
        FOREACH (_ IN CASE WHEN c IS NOT NULL THEN [1] ELSE [] END |
            MERGE (p)-[:LECIONA_NO_CURSO]->(c)
        )
        FOREACH (_ IN CASE WHEN t IS NOT NULL THEN [1] ELSE [] END |
            MERGE (p)-[:ORIENTA_TCC]->(t)
        )
        """
        print(professor_data)
        session.run(query, professor_data)
        print(f"Professor {professor_data['nome_prof']} inserido ou atualizado com sucesso.")



# Função para criar TCC
def create_tcc_in_neo4j(driver, tcc_data):
    with driver.session() as session:
        query = """
        MERGE (t:Tcc {id_tcc: $id_tcc})
        WITH t
        MATCH (a:Aluno {ra: $ra})
        MERGE (t)-[:DESENVOLVIDO_POR]->(a)
        """
        session.run(query, tcc_data)
        print(f"TCC {tcc_data['id_tcc']} inserido ou atualizado com sucesso.")

# ************************* INSERÇÃO DE DADOS PARA FINS DE TESTE **************************************

def migrate_data(postgres_conn, neo4j_driver):
    tables = list_tables_and_columns(postgres_conn)
    for table, columns in tables.items():
        rows = fetch_table_data(postgres_conn, table, columns)
        
        # Dependendo do nome da tabela, chamamos a função de inserção específica
        if table == "aluno":
            for row in rows:
                create_aluno_in_neo4j(neo4j_driver, row)
        elif table == "curso":
            for row in rows:
                create_curso_in_neo4j(neo4j_driver, row)
        elif table == "departamento":
            for row in rows:
                create_departamento_in_neo4j(neo4j_driver, row)
        elif table == "disciplina":
            for row in rows:
                create_disciplina_in_neo4j(neo4j_driver, row)
        elif table == "formados":
            for row in rows:
                create_formados_in_neo4j(neo4j_driver, row)
        elif table == "histdisc":
            for row in rows:
                create_histdisc_in_neo4j(neo4j_driver, row)
        elif table == "histesc":
            for row in rows:
                create_histesc_in_neo4j(neo4j_driver, row)
        elif table == "matrizcurricular":
            for row in rows:
                create_matrizcurricular_in_neo4j(neo4j_driver, row)
        elif table == "professor_departamento":
            for row in rows:
                create_professor_departamento_in_neo4j(neo4j_driver, row)
        elif table == "professor":
            for row in rows:
                create_professor_in_neo4j(neo4j_driver, row)
        elif table == "tcc":
            for row in rows:
                create_tcc_in_neo4j(neo4j_driver, row)
        



# Função para fechar a conexão com o PostgreSQL
def close_postgres(connection):
    if connection:
        connection.close()
        print("Conexão com o PostgreSQL encerrada")

# Função para fechar a conexão com o Neo4j
def close_neo4j(driver):
    if driver:
        driver.close()
        print("Conexão com o Neo4j encerrada")

# Executando o programa
if __name__ == "__main__":
    # Conectando aos bancos de dados
    postgres_conn = connect_postgres()
    neo4j_driver = connect_neo4j()

    if neo4j_driver:
        # Realiza a migração de dados para o Neo4j
        migrate_data(postgres_conn, neo4j_driver)
    
    # Fechando as conexões
    close_postgres(postgres_conn)
    close_neo4j(neo4j_driver)
    
