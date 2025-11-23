import psycopg2
from faker import Faker
import random
import numpy as np
from datetime import timedelta, datetime

fake = Faker("pt_BR")


def fix_utf8(s):
    if isinstance(s, str):
        return s.encode("utf-8", errors="replace").decode("utf-8")
    return s


HIERARQUIA_VALIDA = {
    "Vendas": ["Vendedor", "Supervisor", "Gerente"],
    "Operações": ["Tecnico", "Supervisor", "Gerente"],
    "Suporte Técnico": ["Tecnico", "Supervisor"],
    "Financeiro": ["Supervisor", "Gerente"],     
    "Administrativo": ["Supervisor", "Gerente"],
    "Diretoria": ["Gerente"]                     
}

pesos_departamento = {
    "Vendas": 1.8,
    "Operações": 1.2,
    "Suporte Técnico": 1.6,
    "Financeiro": 0.7,
    "Administrativo": 0.8,
    "Diretoria": 1.3
}

pesos_cargo = {
    "Vendedor": 1.6,
    "Supervisor": 1.2,
    "Gerente": 1.1,
    "Tecnico": 1.4
}

limites_cargo = {
    "Vendedor": 30.0,
    "Supervisor": 80.0,
    "Gerente": 150.0,
    "Tecnico": 120.0
}

DEPARTAMENTOS_FIXOS = list(HIERARQUIA_VALIDA.keys())
CARGOS_FIXOS = list(set([cargo for lista in HIERARQUIA_VALIDA.values() for cargo in lista]))

DISPOSITIVOS = ["Smartphone", "Tablet", "Roteador", "IoT"]
SITUACOES = ["Inativo", "Ativo", "Suspenso"]
EVENTOS = ["Nenhum", "Roaming", "Bloqueio", "Excesso de dados"]
ALERTAS = [False, True]


def inserir_departamentos(cursor):
    for nome in DEPARTAMENTOS_FIXOS:
        # ON CONFLICT para evitar erro se rodar 2x sem limpar
        cursor.execute("INSERT INTO departamentos (nome) VALUES (%s);", (fix_utf8(nome),))

def inserir_cargos(cursor):
    for nome in CARGOS_FIXOS:
        limite = limites_cargo.get(nome, 50.0)
        cursor.execute(
            "INSERT INTO cargos (nome, limite_gigas) VALUES (%s, %s);",
            (fix_utf8(nome), limite)
        )

def inserir_dispositivos(cursor):
    for d in DISPOSITIVOS:
        cursor.execute("INSERT INTO dispositivos (nome_dispositivo) VALUES (%s);", (fix_utf8(d),))

def inserir_situacao(cursor):
    for s in SITUACOES:
        cursor.execute("INSERT INTO situacao (situacao) VALUES (%s);", (fix_utf8(s),))

def inserir_eventos(cursor):
    for e in EVENTOS:
        cursor.execute("INSERT INTO eventos_especiais (nome_eventos) VALUES (%s);", (fix_utf8(e),))

def inserir_alerta_excesso(cursor):
    for a in ALERTAS:
        cursor.execute("INSERT INTO altera_excesso (nome_alerta) VALUES (%s);", (fix_utf8(a),))


def inserir_usuarios(cursor):
    # Busca IDs do banco para mapear
    cursor.execute("SELECT nome, id_departamento FROM departamentos;")
    # Cria dict: {'Vendas': 1, 'TI': 2}
    map_dep = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT nome, id_cargo FROM cargos;")
    # Cria dict: {'Vendedor': 1, 'Gerente': 2}
    map_cargo = {row[0]: row[1] for row in cursor.fetchall()}
    qtd_usuarios = 50 

    for _ in range(qtd_usuarios):
        nome = fix_utf8(fake.name())
        
        nome_dep_escolhido = random.choice(list(HIERARQUIA_VALIDA.keys()))
        id_dep = map_dep[nome_dep_escolhido]
        
        cargos_permitidos = HIERARQUIA_VALIDA[nome_dep_escolhido]
        nome_cargo_escolhido = random.choice(cargos_permitidos)
        id_cargo = map_cargo[nome_cargo_escolhido]

        cursor.execute(
            """INSERT INTO usuario (nome, id_departamento, id_cargo, id_empresa)
               VALUES (%s, %s, %s, 1);""",
            (nome, id_dep, id_cargo)
        )



def inserir_log(cursor):
    cursor.execute("SELECT id_usuario, id_departamento, id_cargo FROM usuario;")
    usuarios = cursor.fetchall()

    if not usuarios:
        print("[AVISO] Nenhum usuário encontrado. Pulei logs.")
        return

    cursor.execute("SELECT id_situacao FROM situacao;")
    situacoes = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_alerta FROM altera_excesso;")
    alertas = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_evento FROM eventos_especiais;")
    eventos = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_dispositivo FROM dispositivos;")
    dispositivos = [r[0] for r in cursor.fetchall()]
    
    cursor.execute("SELECT id_departamento, nome FROM departamentos;")
    map_dep_nome = {r[0]: r[1] for r in cursor.fetchall()}
    
    cursor.execute("SELECT id_cargo, nome FROM cargos;")
    map_cargo_nome = {r[0]: r[1] for r in cursor.fetchall()}

    data_inicio = datetime.now() - timedelta(days=540)
    historico_consumo = {}

    for _ in range(15000):

        id_usuario, id_departamento, id_cargo = random.choice(usuarios)

        dias_passados = random.randint(0, 540)
        data_uso = data_inicio + timedelta(days=dias_passados)
        data_ref = data_uso.date()

        base = np.random.lognormal(mean=0.4, sigma=0.55)

        nome_dep = map_dep_nome.get(id_departamento, "Vendas") 
        nome_cargo = map_cargo_nome.get(id_cargo, "Vendedor")  

        ajuste_dep = pesos_departamento.get(nome_dep, 1.0)
        ajuste_cg = pesos_cargo.get(nome_cargo, 1.0)

        saz = 1.25 if data_uso.weekday() < 5 else 0.75
        tendencia = 1 + dias_passados * 0.002

        consumo_ontem = historico_consumo.get((id_usuario, data_ref - timedelta(days=1)), base)
        autocorr = 0.3 * consumo_ontem

        consumo = base + autocorr
        consumo *= ajuste_dep * ajuste_cg * saz * tendencia
        consumo = round(float(consumo), 2)
        
        if consumo < 0.01: consumo = 0.01

        custo_total = round(consumo * random.uniform(1.5, 3.5), 2)

        historico_consumo[(id_usuario, data_ref)] = consumo

        cursor.execute(
            """
            INSERT INTO log_uso_sim (
                id_usuario, id_situacao, id_alerta, id_evento, id_dispositivo,
                data_uso, consumo_dados_gb, custo_total, localizacao, data_referencia
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (
                id_usuario,
                random.choice(situacoes),
                random.choice(alertas),
                random.choice(eventos),
                random.choice(dispositivos),
                data_uso,
                consumo,
                custo_total,
                fix_utf8(fake.city()),
                data_ref
            )
        )

def main():

    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            user="postgres",
            password="1234",
            database="ANALISE",
            client_encoding="UTF8"
        )

        conn.set_client_encoding("UTF8")
        cursor = conn.cursor()
        print("[OK] Conectado ao banco.")

    except Exception as e:
        print("[ERRO] Falha ao conectar ao PostgreSQL:", e)
        return

    try:
        cursor.execute("INSERT INTO empresas (nome) VALUES ('Empresa X');")
        print("[OK] Empresa inserida.")

        inserir_departamentos(cursor)
        print("[OK] Departamentos inseridos.")

        inserir_cargos(cursor)
        print("[OK] Cargos inseridos.")

        inserir_dispositivos(cursor)
        print("[OK] Dispositivos inseridos.")

        inserir_situacao(cursor)
        print("[OK] Situações inseridas.")

        inserir_eventos(cursor)
        print("[OK] Eventos inseridos.")

        inserir_alerta_excesso(cursor)
        print("[OK] Alertas inseridos.")

        inserir_usuarios(cursor)
        print("[OK] Usuários inseridos (Cargos respeitando Departamentos).")

        inserir_log(cursor)
        print("[OK] Logs de consumo inseridos.")

        conn.commit()
        print("\n[SUCESSO] Todos os dados foram salvos no banco!")

    except Exception as e:
        conn.rollback()
        print("\n[ERRO] Falha durante a execução:", e)

    finally:
        cursor.close()
        conn.close()
        print("[OK] Conexão encerrada.")


if __name__ == "__main__":
    main()