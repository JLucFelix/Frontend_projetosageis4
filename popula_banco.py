import psycopg2
from faker import Faker
import random
import numpy as np
from datetime import timedelta, datetime

fake = Faker("pt_BR")

# ---------- Função para garantir UTF-8 ----------
def fix_utf8(s):
    if isinstance(s, str):
        return s.encode("utf-8", errors="replace").decode("utf-8")
    return s

# ---------- CONFIGURAÇÃO DE VIESES ----------
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

DEPARTAMENTOS_FIXOS = list(pesos_departamento.keys())
CARGOS_FIXOS = list(pesos_cargo.keys())
DISPOSITIVOS = ["Smartphone", "Tablet", "Roteador", "IoT"]
SITUACOES = ["Inativo", "Ativo", "Suspenso"]
EVENTOS = ["Nenhum", "Roaming", "Bloqueio", "Excesso de dados"]
ALERTAS = [False, True]


# ------------------------------------------------------------------
#  INSERÇÃO DAS ENTIDADES FIXAS (com UTF-8 garantido)
# ------------------------------------------------------------------

def inserir_departamentos(cursor):
    for nome in DEPARTAMENTOS_FIXOS:
        cursor.execute("INSERT INTO departamentos (nome) VALUES (%s);", (fix_utf8(nome),))

def inserir_cargos(cursor):
    for nome in CARGOS_FIXOS:
        cursor.execute(
            "INSERT INTO cargos (nome, limite_gigas) VALUES (%s, %s);",
            (fix_utf8(nome), limites_cargo[nome])
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


# ------------------------------------------------------------------
#  INSERÇÃO DE USUÁRIOS (UTF-8 aplicado)
# ------------------------------------------------------------------

def inserir_usuarios(cursor):
    cursor.execute("SELECT id_departamento, nome FROM departamentos;")
    departamentos = cursor.fetchall()

    cursor.execute("SELECT id_cargo, nome FROM cargos;")
    cargos = cursor.fetchall()

    for _ in range(40):
        nome = fix_utf8(fake.name())
        dep_id, dep_nome = random.choice(departamentos)
        cargo_id, cargo_nome = random.choice(cargos)

        cursor.execute(
            """INSERT INTO usuario (nome, id_departamento, id_cargo, id_empresa)
               VALUES (%s, %s, %s, 1);""",
            (nome, dep_id, cargo_id)
        )


# ------------------------------------------------------------------
#  INSERÇÃO DO LOG DE USO (UTF-8 aplicado em cidade/localização)
# ------------------------------------------------------------------

def inserir_log(cursor):
    cursor.execute("SELECT id_usuario, id_departamento, id_cargo FROM usuario;")
    usuarios = cursor.fetchall()

    cursor.execute("SELECT id_situacao FROM situacao;")
    situacoes = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_alerta FROM altera_excesso;")
    alertas = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_evento FROM eventos_especiais;")
    eventos = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_dispositivo FROM dispositivos;")
    dispositivos = [r[0] for r in cursor.fetchall()]

    data_inicio = datetime.now() - timedelta(days=540)

    historico_consumo = {}

    for _ in range(15000):

        id_usuario, id_departamento, id_cargo = random.choice(usuarios)

        # Tempo
        dias_passados = random.randint(0, 540)
        data_uso = data_inicio + timedelta(days=dias_passados)
        data_ref = data_uso.date()

        # Base estatística
        base = np.random.lognormal(mean=0.4, sigma=0.55)

        # Ajustes
        cursor.execute("SELECT nome FROM departamentos WHERE id_departamento=%s", (id_departamento,))
        nome_dep = cursor.fetchone()[0]

        cursor.execute("SELECT nome FROM cargos WHERE id_cargo=%s", (id_cargo,))
        nome_cargo = cursor.fetchone()[0]

        ajuste_dep = pesos_departamento[nome_dep]
        ajuste_cg = pesos_cargo[nome_cargo]

        saz = 1.25 if data_uso.weekday() < 5 else 0.75
        tendencia = 1 + dias_passados * 0.002

        consumo_ontem = historico_consumo.get((id_usuario, data_ref - timedelta(days=1)), base)
        autocorr = 0.3 * consumo_ontem

        consumo = base + autocorr
        consumo *= ajuste_dep * ajuste_cg * saz * tendencia
        consumo = round(float(consumo), 2)

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

    # =============================
    # 1. Conectar ao PostgreSQL
    # =============================
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
        # =============================
        # 2. Empresa base (obrigatória)
        # =============================
        cursor.execute("INSERT INTO empresas (nome) VALUES ('Empresa X') ON CONFLICT DO NOTHING;")
        print("[OK] Empresa inserida.")

        # =============================
        # 3. Entidades fixas
        # =============================
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

        # =============================
        # 4. Usuários
        # =============================
        inserir_usuarios(cursor)
        print("[OK] Usuários inseridos.")

        # =============================
        # 5. Logs de consumo
        # =============================
        inserir_log(cursor)
        print("[OK] Logs de consumo inseridos.")

        # =============================
        # 6. Commit final
        # =============================
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