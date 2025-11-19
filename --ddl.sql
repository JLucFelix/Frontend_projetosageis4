--ddl
-- DDL: criar esquema consistente (idempotente)
DROP TABLE IF EXISTS log_uso_sim CASCADE;
DROP TABLE IF EXISTS usuario CASCADE;
DROP TABLE IF EXISTS altera_excesso CASCADE;
DROP TABLE IF EXISTS eventos_especiais CASCADE;
DROP TABLE IF EXISTS situacao CASCADE;
DROP TABLE IF EXISTS dispositivos CASCADE;
DROP TABLE IF EXISTS cargos CASCADE;
DROP TABLE IF EXISTS departamentos CASCADE;
DROP TABLE IF EXISTS empresas CASCADE;

CREATE TABLE empresas (
    id_empresa SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL
);

CREATE TABLE departamentos (
    id_departamento SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL
);

CREATE TABLE cargos (
    id_cargo SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    limite_gigas FLOAT NOT NULL
);

CREATE TABLE usuario (
    id_usuario SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    id_departamento INT NOT NULL,
    id_cargo INT NOT NULL,
    id_empresa INT NOT NULL,
    FOREIGN KEY (id_departamento) REFERENCES departamentos(id_departamento),
    FOREIGN KEY (id_cargo) REFERENCES cargos(id_cargo),
    FOREIGN KEY (id_empresa) REFERENCES empresas(id_empresa)
);

CREATE TABLE situacao (
    id_situacao SERIAL PRIMARY KEY,
    situacao VARCHAR(100) NOT NULL
);

CREATE TABLE eventos_especiais (
    id_evento SERIAL PRIMARY KEY,
    nome_eventos VARCHAR(100) NOT NULL
);

CREATE TABLE altera_excesso (
     id_alerta SERIAL PRIMARY KEY,
     nome_alerta BOOLEAN NOT NULL
);

CREATE TABLE dispositivos(
    id_dispositivo SERIAL PRIMARY KEY,
    nome_dispositivo VARCHAR(100) NOT NULL
);

CREATE TABLE log_uso_sim (
    id_log SERIAL PRIMARY KEY,
    id_usuario INT NOT NULL,
    id_situacao INT NOT NULL,
    id_alerta INT NOT NULL,
    id_evento INT NOT NULL,
    id_dispositivo INT NOT NULL,
    data_uso TIMESTAMP NOT NULL,
    consumo_dados_gb NUMERIC(10,2) NOT NULL,
    custo_total NUMERIC(10,2),
    localizacao VARCHAR(255),
    data_referencia DATE,
    FOREIGN KEY (id_usuario) REFERENCES usuario(id_usuario),
    FOREIGN KEY (id_situacao) REFERENCES situacao(id_situacao),
    FOREIGN KEY (id_alerta) REFERENCES altera_excesso(id_alerta),
    FOREIGN KEY (id_evento) REFERENCES eventos_especiais(id_evento),
    FOREIGN KEY (id_dispositivo) REFERENCES dispositivos(id_dispositivo)
);
