import streamlit as st

# Configuração deve ser a PRIMEIRA linha
st.set_page_config(
    page_title="Fulltime SIM Dashboard",
    page_icon="🔗",
    layout="wide"
)

# Tenta importar o dashboard
try:
    import dashboard
except ImportError as e:
    st.error(f"Erro ao importar 'dashboard.py'. Verifique se o arquivo está na mesma pasta. Detalhe: {e}")
    st.stop()

# --- BARRA LATERAL ---
with st.sidebar:
    st.title("Navegação")
    page = st.radio(
        "Ir para:",
        ("Página Inicial", "Sobre o Projeto", "Tecnologias", "Sobre Nós", "Dashboard"),
        label_visibility="collapsed"
    )

# --- ROTEAMENTO ---

if page == "Página Inicial":
    st.title("Bem-vindo ao Painel de Controle 🏠")
    st.markdown("""
    ### Sistema de Gestão de SIM Cards
    Utilize o menu lateral para navegar.
    
    - **Dashboard:** Conexão com Banco de Dados e Previsão de IA.
    - **Sobre:** Informações sobre o desenvolvimento.
    """)

elif page == "Sobre o Projeto":
    st.title("Sobre o Projeto 📝")
    st.markdown("Solução desenvolvida para otimizar o controle de dados móveis corporativos.")

elif page == "Tecnologias":
    st.title("Tecnologias Utilizadas 🚀")
    st.markdown("""
    * **Frontend:** Streamlit
    * **Backend:** Python + PostgreSQL
    * **IA:** LightGBM
    * **Visualização:** Plotly
    """)

elif page == "Sobre Nós":
    st.title("Sobre Nós 👥")
    st.write("Equipe de Desenvolvimento Fulltime.")

elif page == "Dashboard":
    # Chama a função que estava dando erro
    # Se dashboard.py estiver correto, isso vai funcionar agora
    if hasattr(dashboard, 'show_dashboard_ui'):
        dashboard.show_dashboard_ui()
    else:
        st.error("Erro Crítico: A função 'show_dashboard_ui' não foi encontrada dentro de 'dashboard.py'. Verifique se você salvou o código correto no arquivo dashboard.py.")