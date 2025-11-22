import streamlit as st
import os
import pandas as pd
import random
from datetime import datetime

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Fulltime | Analytics",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. ESTILO CSS ROBUSTO (Correção de Bugs) ---
def local_css():
    st.markdown("""
        <style>
        /* Importando fonte moderna */
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;700&display=swap');

        /* Forçando variáveis de cor para garantir visibilidade em qualquer tema */
        :root {
            --primary-red: #E60000;
            --dark-red: #B30000;
            --bg-color: #FAFAFA;
            --card-bg: #FFFFFF;
            --text-color: #333333; /* Texto escuro forçado */
            --text-secondary: #666666;
        }

        /* Reset global para garantir fundo claro e texto legível */
        html, body, .stApp {
            background-color: var(--bg-color);
            font-family: 'Roboto', sans-serif;
            color: var(--text-color);
        }

        /* --- SIDEBAR --- */
        [data-testid="stSidebar"] {
            background-color: #FFFFFF;
            border-right: 1px solid #E0E0E0;
        }
        
        /* Corrigindo visibilidade do texto na sidebar */
        [data-testid="stSidebar"] * {
            color: #333333 !important;
        }

        /* --- TEXTOS E TÍTULOS --- */
        h1, h2, h3, h4, h5, h6 {
            color: var(--text-color) !important;
            font-weight: 700 !important;
        }
        
        /* Destaques em vermelho para spans específicos */
        .red-highlight {
            color: var(--primary-red) !important;
        }

        /* --- CARDS (Containers Estilizados) --- */
        div.css-card {
            background-color: var(--card-bg);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-left: 5px solid var(--primary-red);
            margin-bottom: 20px;
            color: var(--text-color);
        }
        
        /* --- METRICS & KPI --- */
        [data-testid="stMetricValue"] {
            color: var(--primary-red) !important;
        }

        /* --- BOTÕES --- */
        .stButton > button {
            background-color: var(--primary-red);
            color: white !important;
            border: none;
            border-radius: 6px;
            font-weight: bold;
        }
        .stButton > button:hover {
            background-color: var(--dark-red);
            color: white !important;
        }
        
        /* Removemos o CSS agressivo dos Radio Buttons para evitar os 'pontos' quebrados.
           O Streamlit nativo cuidará da funcionalidade, apenas ajustamos cores se necessário. */
        
        </style>
    """, unsafe_allow_html=True)

local_css()

# --- 3. BARRA LATERAL ---
with st.sidebar:
    # Tenta carregar a logo, se não existir, mostra texto
    if os.path.exists("logo.png"):
        st.image("logo.png", use_container_width=True)
    else:
        st.markdown(f"""
            <div style='text-align: center; padding: 15px; border-bottom: 2px solid #E60000; margin-bottom: 15px;'>
                <h2 style='color: #E60000; margin:0;'>FULLTIME</h2>
                <p style='font-size: 0.8em; color: #666;'>Analytics Dashboard</p>
            </div>
        """, unsafe_allow_html=True)

    st.header("Navegação")
    
    # Menu simplificado e robusto
    page = st.radio(
        "Ir para:",
        ["Página Inicial", "Dashboard", "Sobre o Projeto", "Tecnologias", "Sobre Nós"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.caption("Versão 1.1.0 | Fulltime")

# --- 4. CONTEÚDO DA PÁGINA ---

if page == "Página Inicial":
    st.markdown("<h1 style='text-align: center;'>Bem-vindo ao <span class='red-highlight'>Fulltime Analytics</span></h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-size: 1.1em; color: #666;'>Central de inteligência para gestão de dados móveis.</p>", unsafe_allow_html=True)
    st.divider()

    # Cards usando HTML container seguro
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000; margin-bottom: 10px;">📊 Monitoramento</h3>
            <p style="font-size: 0.95em;">Acompanhe o consumo de dados em tempo real. Visualize métricas críticas e evite excedentes na fatura.</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000; margin-bottom: 10px;">🤖 Inteligência Artificial</h3>
            <p style="font-size: 0.95em;">Previsões precisas utilizando algoritmos LightGBM para antecipar tendências de uso.</p>
        </div>
        """, unsafe_allow_html=True)
        
    with col3:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000; margin-bottom: 10px;">👥 Gestão de Equipes</h3>
            <p style="font-size: 0.95em;">Controle detalhado por departamentos e colaboradores. Identifique padrões rapidamente.</p>
        </div>
        """, unsafe_allow_html=True)

    st.image("https://images.unsplash.com/photo-1551288049-bebda4e38f71?q=80&w=2070&auto=format&fit=crop", caption="Data Driven Decisions", use_container_width=True)


elif page == "Dashboard":
    st.title("Painel de Controle 📈")
    st.markdown("Visualize os indicadores de performance e consumo.")
    st.divider()
    
    # Tenta importar o módulo dashboard, se falhar, mostra aviso amigável
    try:
        import dashboard
        dashboard.show_dashboard_ui()
    except ImportError:
        st.warning("⚠️ Módulo `dashboard.py` não detectado.")
        st.info("Para visualizar os gráficos, certifique-se que o arquivo de lógica `dashboard.py` está na mesma pasta.")
        
        # Placeholder visual para não ficar vazio
        st.markdown("### Exemplo de Visualização")
        c1, c2, c3 = st.columns(3)
        c1.metric("Consumo Total", "1,240 GB", "12%")
        c2.metric("Previsão Mensal", "1,350 GB", "-5%")
        c3.metric("Linhas Ativas", "450", "2")


elif page == "Sobre o Projeto":
    st.title("Sobre o Projeto 📝")
    st.divider()
    
    col_content, col_side = st.columns([2, 1])
    
    with col_content:
        st.subheader("Solução Complementar ao FullManager")
        st.markdown("""
        Este projeto foi desenvolvido como uma solução para visualização e análise de dados de consumo de SIM cards.
        
        **Objetivos Principais:**
        * Oferecer dashboards interativos para gestores.
        * Proporcionar visão clara sobre o uso de dados por funcionários e departamentos.
        * Facilitar a tomada de decisões estratégicas e controle de custos.
        """)
        
        st.info("💡 Foco total na experiência do usuário e na precisão dos dados.")

    with col_side:
        st.markdown("""
        <div class="css-card" style="text-align: center;">
            <h1 style="font-size: 3rem; margin: 0;">📱</h1>
            <h3 style="color: #E60000;">IoT & Telecom</h3>
            <p>Conectividade Inteligente</p>
        </div>
        """, unsafe_allow_html=True)


elif page == "Tecnologias":
    st.title("Stack Tecnológica 🚀")
    st.markdown("Ferramentas de ponta utilizadas no desenvolvimento.")
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000;">Backend & Dados</h3>
            <ul>
                <li><b>Python:</b> Linguagem core do sistema.</li>
                <li><b>Pandas:</b> Manipulação e análise de dados.</li>
                <li><b>LightGBM:</b> Modelos de Machine Learning.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="css-card">
            <h3 style="color: #E60000;">Frontend & UI</h3>
            <ul>
                <li><b>Streamlit:</b> Framework de aplicação web.</li>
                <li><b>Plotly Express:</b> Gráficos interativos.</li>
                <li><b>CSS3:</b> Estilização personalizada.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)


elif page == "Sobre Nós":
    st.title("Nosso Time 👥")
    st.markdown("A equipe responsável por transformar dados em soluções.")
    st.divider()

    # Dados trazidos do frontend.py original
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

    # Renderização em Grid de 3 colunas
    cols = st.columns(3)

    for i, p in enumerate(participantes):
        with cols[i % 3]:
            # Usando HTML seguro para o card
            st.markdown(f"""
            <div class="css-card" style="text-align: center; border-top: 4px solid #E60000;">
                <h3 style="margin: 0; color: #333;">{p['nome']}</h3>
                <p style="color: #666; font-size: 0.9em; margin-bottom: 5px;">{p['funcao']}</p>
                <small style="color: #999;">RA: {p['ra']}</small>
                <br><br>
                <a href="https://instagram.com/{p['insta']}" target="_blank" style="
                    background-color: #E60000;
                    color: white;
                    padding: 8px 15px;
                    text-decoration: none;
                    border-radius: 20px;
                    font-size: 0.8em;
                    font-weight: bold;">
                    Instagram 📸
                </a>
            </div>
            """, unsafe_allow_html=True)