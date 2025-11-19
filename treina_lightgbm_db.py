# treina_lightgbm_db.py
import psycopg2
import pandas as pd
import lightgbm as lgb
import pickle
from lightgbm import early_stopping, log_evaluation
from datetime import timedelta

def load_data_from_db(conn_params):
    conn = psycopg2.connect(**conn_params)
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
        s.situacao AS situacao,
        l.localizacao
    FROM log_uso_sim l
    JOIN usuario u ON l.id_usuario = u.id_usuario
    JOIN departamentos dep ON u.id_departamento = dep.id_departamento
    JOIN cargos c ON u.id_cargo = c.id_cargo
    JOIN eventos_especiais evt ON l.id_evento = evt.id_evento
    JOIN dispositivos disp ON l.id_dispositivo = disp.id_dispositivo
    JOIN situacao s ON l.id_situacao = s.id_situacao
    ORDER BY l.data_uso;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def feature_engineering(df):
    df['data'] = pd.to_datetime(df['data_uso'])
    df = df.sort_values(['id_usuario', 'data']).reset_index(drop=True)
    df.rename(columns={'consumo': 'consumo_dados_gb'}, inplace=True)

    df['year'] = df['data'].dt.year
    df['month'] = df['data'].dt.month
    df['day'] = df['data'].dt.day
    df['dayofweek'] = df['data'].dt.dayofweek
    df['weekofyear'] = df['data'].dt.isocalendar().week.astype(int)
    df['is_weekend'] = df['dayofweek'].isin([5,6]).astype(int)

    df['lag_1'] = df.groupby('id_usuario')['consumo_dados_gb'].shift(1)
    df['lag_7'] = df.groupby('id_usuario')['consumo_dados_gb'].shift(7)
    df['lag_30'] = df.groupby('id_usuario')['consumo_dados_gb'].shift(30)

    df['rolling_7'] = df.groupby('id_usuario')['consumo_dados_gb'].transform(lambda x: x.rolling(7, min_periods=1).mean())
    df['rolling_30'] = df.groupby('id_usuario')['consumo_dados_gb'].transform(lambda x: x.rolling(30, min_periods=1).mean())

    df = df.dropna().reset_index(drop=True)
    return df

def train_and_save(df, model_path="modelo_lightgbm_consumo.pkl"):
    features = [
        "year", "month", "day", "dayofweek", "weekofyear", "is_weekend",
        "lag_1", "lag_7", "lag_30",
        "rolling_7", "rolling_30",
        "cargo", "departamento", "evento", "dispositivo", "situacao"
    ]
    target = "consumo_dados_gb"
    categorical_cols = ["cargo", "departamento", "evento", "dispositivo", "situacao"]

    for c in categorical_cols:
        df[c] = df[c].astype('category')

    max_date = df['data'].max()
    test_start = max_date - pd.Timedelta(days=30)
    train_df = df[df['data'] < test_start].copy()
    test_df = df[df['data'] >= test_start].copy()
    if train_df.empty or test_df.empty:
        cut = int(len(df) * 0.8)
        train_df = df.iloc[:cut].copy()
        test_df = df.iloc[cut:].copy()

    X_train = train_df[features]
    y_train = train_df[target]
    X_test = test_df[features]
    y_test = test_df[target]

    model = lgb.LGBMRegressor(
        n_estimators=2000,
        learning_rate=0.02,
        max_depth=-1,
        feature_fraction=0.9,
        bagging_fraction=0.8,
        bagging_freq=5,
        objective="regression",
        random_state=42,
    )

    model.fit(
        X_train,
        y_train,
        categorical_feature=categorical_cols,
        eval_set=[(X_test, y_test)],
        eval_metric="mae",
        callbacks=[
            early_stopping(stopping_rounds=100),
            log_evaluation(period=100)
        ]
    )

    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"Modelo salvo em {model_path}")

def main():
    conn_params = {
        "database": "ANALISE",
        "user": "postgres",
        "password": "1234",
        "host": "localhost",
        "port": "5433"
    }

    df = load_data_from_db(conn_params)
    if df.empty:
        raise RuntimeError("DataFrame vazio — verifique população do banco.")

    df_fe = feature_engineering(df)
    if df_fe.empty:
        raise RuntimeError("DataFrame vazio após feature engineering — gere mais dados ou reduza lags.")

    train_and_save(df_fe)



main()