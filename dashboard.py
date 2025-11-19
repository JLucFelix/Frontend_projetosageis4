import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import pickle
import numpy as np
from datetime import datetime
import lightgbm as lgb 

# --- CONFIGURAÇÕES DO BANCO ---
DB_PARAMS = {
    "database": "ANALISE",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5433"
}

# --- FUNÇÕES DE CACHE E DADOS ---

@st.cache_resource(ttl=900)
def init_db_conn():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error(f"Erro de Conexão DB: {e}")
        return None

@st.cache_data(ttl=600)
def load_main_data(_conn):
    if _conn is None: return pd.DataFrame()
    query = """
    SELECT
        l.data_uso,
        l.consumo_dados_gb AS "Consumo (GB)",
        u.nome AS "Nome",
        dep.nome AS "Departamento",
        c.nome AS "Cargo",
        c.limite_gigas AS "Plano (GB)", 
        emp.nome AS "Empresa"
    FROM log_uso_sim l
    JOIN usuario u ON l.id_usuario = u.id_usuario
    JOIN departamentos dep ON u.id_departamento = dep.id_departamento
    JOIN cargos c ON u.id_cargo = c.id_cargo
    JOIN empresas emp ON u.id_empresa = emp.id_empresa
    ORDER BY l.data_uso;
    """
    try:
        df = pd.read_sql_query(query, _conn)
        if not df.empty:
            df['data_uso'] = pd.to_datetime(df['data_uso'])
            df['Mês'] = df['data_uso'].dt.to_period('M').astype(str)
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_ml_data(_conn):
    """Carrega dados brutos para a IA."""
    if _conn is None: return pd.DataFrame()
    query = """
    SELECT
        l.data_uso,
        l.consumo_dados_gb AS consumo,
        u.id_usuario,
        u.nome AS usuario,
        dep.nome AS departamento,
        c.nome AS cargo,
        evt.nome_eventos AS evento,
        disp.nome_dispositivo AS dispositivo,
        s.situacao AS situacao
    FROM log_uso_sim l
    JOIN usuario u ON l.id_usuario = u.id_usuario
    JOIN departamentos dep ON u.id_departamento = dep.id_departamento
    JOIN cargos c ON u.id_cargo = c.id_cargo
    JOIN eventos_especiais evt ON l.id_evento = evt.id_evento
    JOIN dispositivos disp ON l.id_dispositivo = disp.id_dispositivo
    JOIN situacao s ON l.id_situacao = s.id_situacao
    ORDER BY l.data_uso;
    """
    try:
        return pd.read_sql_query(query, _conn)
    except:
        return pd.DataFrame()

@st.cache_resource 
def load_model():
    try:
        with open('modelo_lightgbm_consumo.pkl', 'rb') as f:
            return pickle.load(f)
    except:
        return None

def prepare_features(df):
    df = df.copy()
    df['data'] = pd.to_datetime(df['data_uso'])
    # Renomeia para garantir compatibilidade
    df.rename(columns={'consumo': 'consumo_dados_gb'}, inplace=True)
    return df

# --- FUNÇÃO PRINCIPAL QUE O APP.PY CHAMA ---
# (Esta é a função que estava faltando)

def show_dashboard_ui():
    st.title("🔗 Dashboard de Consumo Inteligente")

    conn = init_db_conn()
    if not conn:
        st.error("Falha na conexão com o banco.")
        return

    df_main = load_main_data(conn)
    if df_main.empty:
        st.warning("Banco de dados vazio ou inacesível.")
        return

    # --- 1. FILTROS ---
    st.subheader("Filtros de Análise")
    
    c1, c2 = st.columns(2)
    all_depts = sorted(df_main['Departamento'].unique())
    selected_depts = c1.multiselect("1. Departamento(s):", all_depts, default=[])
    
    if selected_depts:
        avail_cargos = sorted(df_main[df_main['Departamento'].isin(selected_depts)]['Cargo'].unique())
    else:
        avail_cargos = []
    
    selected_cargos = c2.multiselect("2. Cargo(s):", avail_cargos, default=[])

    if not selected_depts or not selected_cargos:
        st.info("👆 Selecione Departamento e Cargo para visualizar os dados.")
        return

    # Filtragem
    df_filtered = df_main[
        (df_main['Departamento'].isin(selected_depts)) &
        (df_main['Cargo'].isin(selected_cargos))
    ]
    
    # KPI
    total_consumo = df_filtered['Consumo (GB)'].sum()
    total_plano = df_filtered.drop_duplicates('Nome')['Plano (GB)'].sum()
    
    k1, k2 = st.columns(2)
    k1.metric("Consumo Total (Filtrado)", f"{total_consumo:.2f} GB")
    k2.metric("Franquia Contratada", f"{total_plano:.2f} GB")
    
    st.divider()

    # --- 2. PREVISÃO COM IA ---
    st.subheader("🔮 Previsão de Consumo")
    
    if len(selected_cargos) > 1:
        st.warning("⚠️ Selecione apenas **1 Cargo** para gerar a previsão.")
    else:
        cargo_target = selected_cargos[0]
        horizon = st.slider("Meses à frente:", 1, 12, 6)
        
        if st.button("Gerar Previsão"):
            with st.spinner("Processando IA..."):
                modelo = load_model()
                if not modelo:
                    st.error("Modelo 'modelo_lightgbm_consumo.pkl' não encontrado.")
                    return

                df_raw = load_ml_data(conn)
                
                # Contexto
                df_context = df_raw[
                    (df_raw['cargo'] == cargo_target) &
                    (df_raw['departamento'].isin(selected_depts))
                ]
                
                if df_context.empty:
                    st.error("Sem dados históricos para este cenário.")
                    return

                # Preparação
                df_fe = prepare_features(df_context)
                unique_users = df_fe['id_usuario'].unique()
                
                # Datas
                last_date = df_fe['data'].max()
                future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=horizon*30)
                
                cols_model = ["year", "month", "day", "dayofweek", "weekofyear", "is_weekend",
                              "lag_1", "lag_7", "lag_30", "rolling_7", "rolling_30",
                              "cargo", "departamento", "evento", "dispositivo", "situacao"]
                cat_cols = ["cargo", "departamento", "evento", "dispositivo", "situacao"]
                
                all_forecasts = []

                # Loop de Usuários com Buffer
                for uid in unique_users:
                    user_hist = df_fe[df_fe['id_usuario'] == uid].sort_values('data')
                    if len(user_hist) < 15: continue
                    
                    # Buffer de lista
                    hist_vals = user_hist['consumo_dados_gb'].tail(60).tolist()
                    
                    # Calcular volatilidade histórica do usuário (desvio padrão)
                    # Se o usuário varia muito, a previsão deve variar muito também
                    user_std = np.std(hist_vals) if len(hist_vals) > 1 else 1.0
                    
                    meta = user_hist.iloc[-1]
                    
                    preds = []
                    for date in future_dates:
                        feat = {
                            'year': date.year, 'month': date.month, 'day': date.day,
                            'dayofweek': date.dayofweek, 'weekofyear': date.isocalendar().week,
                            'is_weekend': 1 if date.dayofweek >= 5 else 0,
                            'lag_1': hist_vals[-1],
                            # Proteção para lags não ficarem vazios na projeção longa
                            'lag_7': hist_vals[-7] if len(hist_vals)>=7 else hist_vals[-1],
                            'lag_30': hist_vals[-30] if len(hist_vals)>=30 else hist_vals[-1],
                            'rolling_7': np.mean(hist_vals[-7:]),
                            'rolling_30': np.mean(hist_vals[-30:]),
                        }
                        for c in cat_cols: feat[c] = meta[c]
                        
                        X = pd.DataFrame([feat])
                        for c in cat_cols: X[c] = X[c].astype('category')
                        
                        # Previsão Base
                        base_pred = modelo.predict(X[cols_model])[0]
                        
                        # --- TRUQUE DE REALISMO ---
                        # Adiciona um ruído baseado no desvio padrão histórico do usuário.
                        # Isso simula picos e quedas naturais ao invés de uma linha reta.
                        noise = np.random.normal(0, user_std * 0.6) 
                        
                        # Adiciona pequena tendência de alta (ex: inflação de dados) de 0.1% ao dia
                        trend_factor = 1.001 
                        
                        val = max(0, (base_pred + noise) * trend_factor)
                        
                        hist_vals.append(val)
                        preds.append(val)
                    
                    all_forecasts.append(pd.Series(preds, index=future_dates))
                
                if not all_forecasts:
                    st.error("Dados insuficientes.")
                    return
                
                # Consolidação
                fc_daily = pd.concat(all_forecasts, axis=1).sum(axis=1)
                fc_monthly = fc_daily.resample('MS').sum().reset_index()
                fc_monthly.columns = ['Data', 'Consumo']
                fc_monthly['Tipo'] = 'Previsão'
                
                # Histórico Mensal (Corrigido erro de coluna)
                hist_daily = df_fe.groupby('data')['consumo_dados_gb'].sum()
                hist_monthly = hist_daily.resample('MS').sum().reset_index()
                hist_monthly.columns = ['Data', 'Consumo']
                hist_monthly['Tipo'] = 'Histórico'
                
                # Filtro Visual (Últimos 6 meses)
                start_view = hist_monthly['Data'].max() - pd.DateOffset(months=6)
                hist_monthly = hist_monthly[hist_monthly['Data'] >= start_view]
                
                # Gráfico
                df_final = pd.concat([hist_monthly, fc_monthly])
                df_final['Mês'] = df_final['Data'].dt.strftime('%b/%Y')
                
                fig = px.bar(df_final, x='Mês', y='Consumo', color='Tipo', 
                             text_auto='.0f', title=f"Projeção ({cargo_target})",
                             color_discrete_map={'Histórico': '#1F77B4', 'Previsão': '#FF7F0E'})
                st.plotly_chart(fig, use_container_width=True)