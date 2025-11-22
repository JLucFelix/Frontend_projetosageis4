import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import pickle
import numpy as np
from datetime import datetime, date
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
    df.rename(columns={'consumo': 'consumo_dados_gb'}, inplace=True)
    return df

# --- FUNÇÃO: DETETIVE DE CAUSAS ---
def analyze_root_cause(df_history, forecast_val, df_raw_context):
    # 1. Análise Estatística
    recent_avg = df_history['Consumo'].mean()
    recent_std = df_history['Consumo'].std()
    
    if np.isnan(recent_std) or recent_std == 0: recent_std = 1.0

    threshold_warning = recent_avg + (1.2 * recent_std) 
    threshold_critical = recent_avg + (2.0 * recent_std) 
    
    avg_forecast = forecast_val
    comparison_base = recent_avg if recent_avg > 0 else 1
    pct_diff = ((avg_forecast / comparison_base) - 1) * 100

    if avg_forecast > threshold_critical:
        status = "CRITICAL"
        color = "red"
        msg = f"🚨 Anomalia Crítica (+{pct_diff:.1f}% vs Média)"
    elif avg_forecast > threshold_warning:
        status = "WARNING"
        color = "orange"
        msg = f"⚠️ Tendência de Alta (+{pct_diff:.1f}%)"
    else:
        status = "NORMAL"
        color = "green"
        msg = "✅ Consumo Projetado dentro da Normalidade"

    # 2. Composição
    causes = []
    total_vol = df_raw_context['consumo'].sum()
    
    if total_vol == 0: return status, color, msg, causes

    # A. Top Usuários
    top_users = df_raw_context.groupby('usuario')['consumo'].sum().sort_values(ascending=False).head(3)
    for user, vol in top_users.items():
        share = (vol / total_vol) * 100
        if share > 99:
             causes.append(f"👤 **Usuário Único:** *{user}* é o único colaborador encontrado com registros neste filtro (100% do volume).")
        elif share > 20: 
            causes.append(f"👤 **Principal Usuário:** *{user}* concentra **{share:.1f}%** do consumo histórico analisado.")

    # B. Dispositivos
    if 'dispositivo' in df_raw_context.columns:
        top_devices = df_raw_context.groupby('dispositivo')['consumo'].sum().sort_values(ascending=False).head(1)
        for dev, vol in top_devices.items():
            share = (vol / total_vol) * 100
            if share > 30:
                causes.append(f"📱 **Perfil de Hardware:** A maior parte do tráfego vem de dispositivos tipo *{dev}* ({share:.0f}%).")

    # C. Roaming
    if 'situacao' in df_raw_context.columns:
        risky_situations = df_raw_context[
            df_raw_context['situacao'].str.contains('Roaming|Excesso|Bloqueado', case=False, na=False)
        ]
        if not risky_situations.empty:
            vol_risk = risky_situations['consumo'].sum()
            share_risk = (vol_risk / total_vol) * 100
            if share_risk > 1: 
                causes.append(f"🌍 **Atenção de Status:** Detectado consumo em *Roaming/Excesso* representando {share_risk:.1f}% do total.")

    # D. Eventos
    if 'evento' in df_raw_context.columns:
        event_days = df_raw_context[
            (df_raw_context['evento'].notna()) & 
            (df_raw_context['evento'] != 'Nenhum')
        ]['consumo'].sum()
        if event_days > 0:
            causes.append("📅 **Sazonalidade:** O histórico contém Eventos Especiais que influenciam o cálculo.")

    # E. Fim de Semana
    df_raw_context['is_weekend'] = pd.to_datetime(df_raw_context['data_uso']).dt.dayofweek >= 5
    if not df_raw_context[df_raw_context['is_weekend']].empty:
        weekend_vol = df_raw_context[df_raw_context['is_weekend']]['consumo'].sum()
        weekend_share = (weekend_vol / total_vol) * 100
        if weekend_share > 20:
            causes.append(f"📆 **Padrão Temporal:** {weekend_share:.0f}% do consumo ocorre aos finais de semana.")

    if not causes:
        causes.append("📈 **Crescimento Orgânico:** Aumento de volume distribuído, sem um ofensor isolado.")

    return status, color, msg, causes

# --- UI PRINCIPAL ---
def show_dashboard_ui():
    st.title("🔗 Dashboard de Previsão Inteligente")

    conn = init_db_conn()
    if not conn:
        st.error("Falha na conexão com o banco.")
        return

    df_main = load_main_data(conn)
    if df_main.empty:
        st.warning("Banco de dados vazio ou inacessível.")
        return

    # --- FILTROS ---
    st.subheader("Filtros de Cenário")
    c1, c2 = st.columns(2)
    all_depts = sorted(df_main['Departamento'].unique())
    selected_depts = c1.multiselect("1. Departamento(s):", all_depts, default=[])
    
    if selected_depts:
        avail_cargos = sorted(df_main[df_main['Departamento'].isin(selected_depts)]['Cargo'].unique())
    else:
        avail_cargos = []
    selected_cargos = c2.multiselect("2. Cargo (Alvo da IA):", avail_cargos, default=[])

    if not selected_depts or not selected_cargos:
        st.info("👆 Selecione Departamento e Cargo para habilitar a IA.")
        if 'forecast_done' in st.session_state: del st.session_state['forecast_done']
        return

    df_filtered = df_main[
        (df_main['Departamento'].isin(selected_depts)) &
        (df_main['Cargo'].isin(selected_cargos))
    ]
    st.metric("Histórico Total do Filtro", f"{df_filtered['Consumo (GB)'].sum():.2f} GB")
    st.divider()

    # --- GERAÇÃO DE PREVISÃO ---
    st.subheader("🔮 Gerar Previsão")
    
    if len(selected_cargos) > 1:
        st.warning("⚠️ Selecione apenas **1 Cargo**.")
    else:
        cargo_target = selected_cargos[0]
        col_in1, col_in2 = st.columns(2)
        horizon = col_in1.slider("Projetar meses:", 1, 12, 6)
        
        if st.button("Gerar Previsão", type="primary"):
            with st.spinner("Processando algoritmos LightGBM..."):
                modelo = load_model()
                if not modelo:
                    st.error("Modelo não encontrado.")
                    return

                df_raw = load_ml_data(conn)
                df_context = df_raw[
                    (df_raw['cargo'] == cargo_target) &
                    (df_raw['departamento'].isin(selected_depts))
                ]
                
                if df_context.empty:
                    st.error("Sem dados.")
                    return

                df_fe = prepare_features(df_context)
                unique_users = df_fe['id_usuario'].unique()
                last_date = df_fe['data'].max()
                future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=horizon*30)
                
                cols_model = ["year", "month", "day", "dayofweek", "weekofyear", "is_weekend",
                              "lag_1", "lag_7", "lag_30", "rolling_7", "rolling_30",
                              "cargo", "departamento", "evento", "dispositivo", "situacao"]
                cat_cols = ["cargo", "departamento", "evento", "dispositivo", "situacao"]
                
                all_forecasts = []
                for uid in unique_users:
                    user_hist = df_fe[df_fe['id_usuario'] == uid].sort_values('data')
                    if len(user_hist) < 15: continue
                    hist_vals = user_hist['consumo_dados_gb'].tail(60).tolist()
                    user_std = np.std(hist_vals) if len(hist_vals) > 1 else 1.0
                    meta = user_hist.iloc[-1]
                    preds = []
                    for date_fc in future_dates:
                        feat = {
                            'year': date_fc.year, 'month': date_fc.month, 'day': date_fc.day,
                            'dayofweek': date_fc.dayofweek, 'weekofyear': date_fc.isocalendar().week,
                            'is_weekend': 1 if date_fc.dayofweek >= 5 else 0,
                            'lag_1': hist_vals[-1],
                            'lag_7': hist_vals[-7] if len(hist_vals)>=7 else hist_vals[-1],
                            'lag_30': hist_vals[-30] if len(hist_vals)>=30 else hist_vals[-1],
                            'rolling_7': np.mean(hist_vals[-7:]), 'rolling_30': np.mean(hist_vals[-30:]),
                        }
                        for c in cat_cols: feat[c] = meta[c]
                        X = pd.DataFrame([feat])
                        for c in cat_cols: X[c] = X[c].astype('category')
                        base_pred = modelo.predict(X[cols_model])[0]
                        noise = np.random.normal(0, user_std * 0.6) 
                        val = max(0, (base_pred + noise) * 1.001)
                        hist_vals.append(val)
                        preds.append(val)
                    all_forecasts.append(pd.Series(preds, index=future_dates))
                
                if all_forecasts:
                    fc_daily = pd.concat(all_forecasts, axis=1).sum(axis=1)
                    fc_monthly = fc_daily.resample('MS').sum().reset_index()
                    fc_monthly.columns = ['Data', 'Consumo']
                    fc_monthly['Tipo'] = 'Previsão'
                    
                    hist_daily = df_fe.groupby('data')['consumo_dados_gb'].sum()
                    hist_monthly = hist_daily.resample('MS').sum().reset_index()
                    hist_monthly.columns = ['Data', 'Consumo'] 
                    hist_monthly['Tipo'] = 'Histórico'

                    st.session_state['fc_data'] = fc_monthly
                    st.session_state['hist_data'] = hist_monthly
                    st.session_state['raw_context'] = df_context
                    st.session_state['target_cargo'] = cargo_target
                    st.session_state['forecast_done'] = True
                    st.success("Previsão Gerada!")
                else:
                    st.error("Dados insuficientes.")

        # --- VISUALIZAÇÃO ---
        if st.session_state.get('forecast_done'):
            
            fc_monthly = st.session_state['fc_data']
            hist_monthly = st.session_state['hist_data']
            df_raw_context = st.session_state['raw_context']
            cargo_label = st.session_state['target_cargo']

            # Diagnóstico
            st.markdown("### 🕵️ Diagnóstico e Composição")
            forecast_avg_val = fc_monthly['Consumo'].mean()
            status, color, msg, causes = analyze_root_cause(hist_monthly, forecast_avg_val, df_raw_context)
            
            if status == "NORMAL": st.success(msg, icon="✅")
            elif status == "WARNING": st.warning(msg, icon="⚠️")
            else: st.error(msg, icon="🚨")
            
            if causes:
                with st.expander("🔍 Ver Detalhes da Composição (Dispositivos, Usuários, etc.)", expanded=True):
                    for cause in causes: st.markdown(f"- {cause}")
                    st.caption("Análise baseada nos padrões históricos associados a este cargo.")

            st.divider()

            # Gráficos
            st.markdown("#### Análise Gráfica")
            last_3_months = hist_monthly.tail(3).copy()
            avg_hist = hist_monthly['Consumo'].mean()

            c_vis1, c_vis2 = st.columns([1, 3])
            with c_vis1:
                tipo_grafico = st.radio(
                    "Visualização:",
                    ["Tendência Conectada", "Volumetria vs Média", "Variação % (MoM)"]
                )
                st.markdown("---")
                st.metric("Total Previsto", f"{fc_monthly['Consumo'].sum():.0f} GB")

            with c_vis2:
                
                # --- GRÁFICO 1: TENDÊNCIA CONECTADA ---
                if tipo_grafico == "Tendência Conectada":
                    fig = go.Figure()
                    
                    # Trace Histórico (com Hover personalizado)
                    fig.add_trace(go.Scatter(
                        x=last_3_months['Data'], 
                        y=last_3_months['Consumo'],
                        mode='lines+markers', 
                        name='Histórico Recente', 
                        line=dict(color='#1F77B4', width=3),
                        hovertemplate="<b>📅 Mês:</b> %{x|%b/%Y}<br><b>📼 Tipo:</b> Histórico Real<br><b>📉 Consumo:</b> %{y:.0f} GB<br><i>Dados reais do banco de dados.</i><extra></extra>"
                    ))
                    
                    # Trace Previsão
                    connect_point = last_3_months.iloc[-1:]
                    fc_connected = pd.concat([connect_point, fc_monthly])
                    
                    fig.add_trace(go.Scatter(
                        x=fc_connected['Data'], 
                        y=fc_connected['Consumo'],
                        mode='lines+markers', 
                        name='Projeção IA', 
                        line=dict(color='#E60000', width=3, dash='dot'),
                        hovertemplate="<b>📅 Mês:</b> %{x|%b/%Y}<br><b>🔮 Tipo:</b> Projeção IA<br><b>🚀 Estimativa:</b> %{y:.0f} GB<br><i>Valor calculado pelo algoritmo LightGBM.</i><extra></extra>"
                    ))
                    
                    fig.update_layout(title=f"Trajetória: {cargo_label}", xaxis_title="Mês", yaxis_title="GB")
                    st.plotly_chart(fig, use_container_width=True)
                
                # --- GRÁFICO 2: VOLUMETRIA vs MÉDIA ---
                elif tipo_grafico == "Volumetria vs Média":
                    fig = go.Figure()
                    
                    # Dados customizados para o Hover (Cálculo da diferença)
                    diffs = fc_monthly['Consumo'] - avg_hist
                    status_text = ["Acima da média" if d > 0 else "Abaixo da média" for d in diffs]
                    
                    fig.add_trace(go.Bar(
                        x=fc_monthly['Data'].dt.strftime('%b/%Y'), 
                        y=fc_monthly['Consumo'],
                        name='Previsão', 
                        marker_color='#E60000', 
                        text=fc_monthly['Consumo'],
                        texttemplate='%{text:.0f}',
                        textposition='auto',
                        # Passamos dados extras para o tooltip
                        customdata=np.stack((diffs, status_text), axis=-1),
                        hovertemplate="<b>📅 %{x}</b><br>📦 <b>Volume:</b> %{y:.0f} GB<br>📏 <b>Média Histórica:</b> " + f"{avg_hist:.0f} GB" + "<br>⚖️ <b>Análise:</b> %{customdata[0]:.0f} GB (%{customdata[1]})<extra></extra>"
                    ))
                    
                    fig.add_hline(y=avg_hist, line_dash="dash", line_color="gray", annotation_text="Média Histórica")
                    fig.update_layout(title=f"Volume vs Média ({avg_hist:.0f} GB)")
                    st.plotly_chart(fig, use_container_width=True)
                    
                # --- GRÁFICO 3: VARIAÇÃO % (MoM) ---
                elif tipo_grafico == "Variação % (MoM)":
                    df_pct = fc_monthly.copy()
                    last_real = last_3_months.iloc[-1]['Consumo']
                    df_pct = pd.concat([pd.DataFrame({'Consumo': [last_real]}), df_pct], ignore_index=True)
                    df_pct['Variação %'] = df_pct['Consumo'].pct_change() * 100
                    df_pct = df_pct.dropna()
                    df_pct['Data'] = fc_monthly['Data'].values
                    
                    # Cria texto explicativo para o hover
                    hover_texts = []
                    for val in df_pct['Variação %']:
                        trend = "Aumento" if val > 0 else "Redução"
                        hover_texts.append(f"{trend} projetada de {abs(val):.1f}% em relação ao mês anterior.")

                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=df_pct['Data'].dt.strftime('%b/%Y'), 
                        y=df_pct['Variação %'],
                        marker_color=df_pct['Variação %'].apply(lambda x: '#2ca02c' if x < 0 else '#d62728'),
                        # Texto visual na barra (limpo)
                        text=df_pct['Variação %'],
                        texttemplate='%{text:+.1f}%', 
                        textposition='outside',
                        # Texto explicativo no mouse (detalhado)
                        customdata=hover_texts,
                        hovertemplate="<b>📅 %{x}</b><br>📊 <b>Variação:</b> %{y:+.1f}%<br>📝 <b>Significado:</b> %{customdata}<extra></extra>"
                    ))
                    
                    # Trava o zoom para não ficar "gigante" se houver outlier
                    max_val = df_pct['Variação %'].abs().max()
                    limit = max(50, max_val * 1.2) # Dá uma margem de respiro
                    
                    fig.update_layout(
                        title="Variação Mensal (%) - Aceleração do Consumo",
                        yaxis_title="Variação vs Mês Anterior",
                        yaxis_range=[-limit, limit] # Centraliza o zero
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)

                    # Texto de apoio abaixo
                    if not df_pct.empty:
                        max_increase = df_pct['Variação %'].max()
                        max_decrease = df_pct['Variação %'].min()
                        idx_max = df_pct['Variação %'].idxmax()
                        month_max = df_pct.loc[idx_max, 'Data'].strftime('%B')
                        
                        texto_analise = "##### 📝 Análise de Tendência:\n"
                        if max_increase > 0:
                            texto_analise += f"- O pico de aceleração está previsto para **{month_max}**, com um salto de **+{max_increase:.1f}%**.\n"
                        else:
                            texto_analise += "- Tendência predominante de estabilidade ou queda.\n"
                            
                        if max_decrease < -5:
                            texto_analise += f"- Nota-se uma redução significativa de **{max_decrease:.1f}%** em determinado momento."
                        elif max_increase < 1 and max_decrease > -1:
                            texto_analise += "- O consumo apresenta estabilidade quase total."
                            
                        st.info(texto_analise)