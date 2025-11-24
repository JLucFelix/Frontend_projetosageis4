import streamlit as st
import os
import pandas as pd
import psycopg2
from streamlit_option_menu import option_menu

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Fulltime | Analytics",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. FUNÇÕES DE BANCO DE DADOS (CORRIGIDO) ---
# REMOVI O @st.cache_resource PARA EVITAR O ERRO "CONNECTION CLOSED"
def init_connection():
    """
    Estabelece a conexão com o banco PostgreSQL.
    Retorna None se falhar.
    """
    try:
        return psycopg2.connect(
            host="localhost",
            port="5433",
            user="postgres",
            password="1234",
            database="ANALISE"
        )
    except Exception:
        return None

def get_kpis_from_db():
    """
    Busca métricas reais. 
    Se falhar, retorna 0 (Zero). Não inventa dados.
    """
    conn = init_connection()

    # Inicializa zerado
    dados = {
        "usuarios": 0,
        "consumo_hoje": 0.0,
        "alertas": 0,
        "status": "Offline" 
    }

    if conn:
        try:
            cur = conn.cursor()

            # 1. Total de Usuários
            cur.execute("SELECT COUNT(*) FROM usuario;")
            result_users = cur.fetchone()
            if result_users:
                dados["usuarios"] = result_users[0]

            # 2. Consumo
            cur.execute("""
                SELECT SUM(consumo_dados_gb)
                FROM log_uso_sim
                WHERE data_referencia = (SELECT MAX(data_referencia) FROM log_uso_sim);
            """)
            result_consumo = cur.fetchone()
            if result_consumo and result_consumo[0]:
                dados["consumo_hoje"] = result_consumo[0]

            # 3. Alertas
            cur.execute("""
                SELECT COUNT(*)
                FROM log_uso_sim l
                JOIN altera_excesso a ON l.id_alerta = a.id_alerta
                WHERE a.nome_alerta = 'True';
            """)
            result_alertas = cur.fetchone()
            if result_alertas:
                dados["alertas"] = result_alertas[0]

            dados["status"] = "Online"
            cur.close()
            conn.close()
        except Exception as e:
            st.error(f"Erro na query SQL: {e}")
            if conn:
                conn.close()

    return dados

# --- 3. ESTILO CSS ---
def local_css():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap');

        :root {
            --primary-red: #E60000;
            --dark-red: #B30000;
            --bg-color: #FAFAFA;
            --card-bg: #FFFFFF;
            --text-color: #333333;
        }

        html, body, .stApp {
            background-color: var(--bg-color);
            font-family: 'Roboto', sans-serif;
            color: var(--text-color);
        }

        [data-testid="stSidebar"] {
            background-color: #FFFFFF;
            border-right: 1px solid #E0E0E0;
        }
        
        div.css-card {
            background-color: var(--card-bg);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-left: 5px solid var(--primary-red);
            margin-bottom: 20px;
            color: var(--text-color);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }

        div.css-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 20px rgba(230, 0, 0, 0.15);
        }

        h1, h2, h3, h4, h5, h6 {
            color: var(--text-color) !important;
            font-weight: 700 !important;
        }
        
        .red-highlight { color: var(--primary-red) !important; }
        [data-testid="stMetricValue"] { color: var(--primary-red) !important; }

        a.custom-btn {
            background-color: var(--primary-red);
            color: white !important;
            padding: 8px 15px;
            text-decoration: none;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            display: inline-block;
            transition: background 0.3s;
        }
        a.custom-btn:hover { background-color: var(--dark-red); }
        </style>
    """, unsafe_allow_html=True)

local_css()

# --- 4. BARRA LATERAL ---
with st.sidebar:
    if os.path.exists("logo.png"):
        st.image("logo.png", use_container_width=True)
    else:
        st.markdown(f"""
            <div style='text-align: center; padding: 15px; border-bottom: 2px solid #E60000; margin-bottom: 15px;'>
                <h2 style='color: #E60000; margin:0;'>FULLTIME</h2>
                <p style='font-size: 0.8em; color: #666;'>Analytics Dashboard</p>
            </div>
        """, unsafe_allow_html=True)

    selected = option_menu(
        menu_title=None, 
        options=["Página Inicial", "Dashboard", "Sobre o Projeto", "Tecnologias", "Sobre Nós"],
        icons=["house", "graph-up", "file-text", "cpu", "people"],
        default_index=0,
        styles={
            "nav-link-selected": {"background-color": "#E60000"},
            "nav-link": {"font-family": "Roboto", "font-size": "14px"}
        }
    )

    st.markdown("---")
    st.caption("Versão 1.2.1 | Fulltime")
    
    # Verificação de status na Sidebar
    db_conn = init_connection()
    if db_conn:
        st.success("Conectado ao BD")
        db_conn.close()
    else:
        st.error("BD desconectado")

# --- 5. CONTEÚDO ---

if selected == "Página Inicial":
    st.markdown("<h1 style='text-align: center;'>Bem-vindo ao <span class='red-highlight'>Fulltime Analytics</span></h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-size: 1.1em; color: #666;'>Central de inteligência para gestão de dados móveis.</p>", unsafe_allow_html=True)
    
    st.markdown("### ⚡ Visão Rápida")
    
    kpis = get_kpis_from_db()

    k1, k2, k3 = st.columns(3)
    k1.metric("Sim Cards Ativos", f"{kpis['usuarios']}", kpis['status'])
    k2.metric("Consumo Hoje", f"{kpis['consumo_hoje']:.1f} GB", "Dados")
    k3.metric("Alertas de Excesso", f"{kpis['alertas']}", "Crítico", delta_color="inverse")
    
    if kpis['status'] == "Offline":
        st.warning("⚠️ O sistema não detectou conexão com o banco de dados 'ANALISE'. Os valores acima estão zerados.")

    st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000; margin-bottom: 10px;">📊 Monitoramento</h3>
            <p style="font-size: 0.95em;">Acompanhe o consumo de dados em tempo real.</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000; margin-bottom: 10px;">🤖 Inteligência Artificial</h3>
            <p style="font-size: 0.95em;">Previsões precisas utilizando algoritmos LightGBM.</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000; margin-bottom: 10px;">👥 Gestão de Equipes</h3>
            <p style="font-size: 0.95em;">Controle detalhado por departamentos e colaboradores.</p>
        </div>
        """, unsafe_allow_html=True)


elif selected == "Dashboard":
    st.title("Painel de Controle 📈")
    st.markdown("Visualize os indicadores de performance e consumo.")
    st.divider()

    try:
        import dashboard
        dashboard.show_dashboard_ui()
    except ImportError:
        conn = init_connection()
        if conn:
            st.markdown("#### Consumo Real por Departamento")
            try:
                query = """
                SELECT d.nome, SUM(l.consumo_dados_gb) as total
                FROM log_uso_sim l
                JOIN usuario u ON l.id_usuario = u.id_usuario
                JOIN departamentos d ON u.id_departamento = d.id_departamento
                GROUP BY d.nome
                ORDER BY total DESC
                """
                df_chart = pd.read_sql(query, conn)
                st.bar_chart(df_chart, x="nome", y="total", color="#E60000")
                conn.close()
            except Exception as e:
                st.error(f"Erro ao executar query de dashboard: {e}")
                conn.close()
        else:
            st.error("🚫 Falha na conexão com o Banco de Dados.")


elif selected == "Sobre o Projeto":
    st.title("Sobre o Projeto 📝")
    st.divider()
    
    col_content, col_side = st.columns([2, 1])
    
    with col_content:
        st.subheader("Solução Complementar ao FullManager")
        st.markdown("""
        Este projeto foi desenvolvido como uma solução para visualização e análise de dados.
        """)

    with col_side:
        st.markdown("""
        <div class="css-card" style="text-align: center;">
            <h1 style="font-size: 3rem; margin: 0;">📱</h1>
            <h3 style="color: #E60000;">IoT & Telecom</h3>
        </div>
        """, unsafe_allow_html=True)


elif selected == "Tecnologias":
    st.title("Stack Tecnológica 🚀")
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000;">Backend & Dados</h3>
            <ul>
                <li><b>Python, Pandas, PostgreSQL</b></li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000;">Frontend & UI</h3>
            <ul>
                <li><b>Streamlit, Plotly, CSS3</b></li>
            </ul>
        </div>
        """, unsafe_allow_html=True)


elif selected == "Sobre Nós":
    st.title("Nosso Time 👥")
    st.divider()

    participantes = [
        {"nome": "Diego", "ra": "1989361", "funcao": "Back-end e IA", "insta": "diegoartero_"},
        {"nome": "Fernando", "ra": "1990340", "funcao": "Banco de dados", "insta": "fernandocaffer"},
        {"nome": "Guilherme", "ra": "1991991", "funcao": "Front-end", "insta": "guilherme.morrone"},
        {"nome": "Henrique", "ra": "1992437", "funcao": "Documentação", "insta": "rick.grram"},
        {"nome": "Jean", "ra": "2012388", "funcao": "Front-end", "insta": "jeanlucflx"},
        {"nome": "João", "ra": "1993739", "funcao": "QA & Dados", "insta": "Nissimura_"},
        {"nome": "Kaique", "ra": "1994836", "funcao": "QA & Dados", "insta": "kaikerenan11"},
        {"nome": "Leonardo", "ra": "1995657", "funcao": "QA & Dados", "insta": "toledx"},
        {"nome": "Maria Elisa", "ra": "2013350", "funcao": "Front-end", "insta": "mary_elisa7"}
    ]

    cols = st.columns(3)

    for i, p in enumerate(participantes):
        with cols[i % 3]:
            st.markdown(f"""
            <div class="css-card" style="text-align: center; border-top: 4px solid #E60000;">
                <h3 style="margin: 0; color: #333;">{p['nome']}</h3>
                <p style="color: #666; font-size: 0.9em; margin-bottom: 5px;">{p['funcao']}</p>
                <small style="color: #999;">RA: {p['ra']}</small>
                <br><br>
                <a href="https://instagram.com/{p['insta']}" target="_blank" class="custom-btn">
                    Instagram 📸
                </a>
            </div>
            """, unsafe_allow_html=True)