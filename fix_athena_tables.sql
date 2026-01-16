-- ============================================================================
-- Script para corrigir tabelas do Athena com problemas
-- ============================================================================

-- 1. DROPAR TABELAS CORROMPIDAS
DROP TABLE IF EXISTS default.refined_stocks;
DROP TABLE IF EXISTS default.aggregated_stocks_monthly;

-- 2. RECRIAR TABELA REFINED_STOCKS (ajuste o bucket conforme necessário)
CREATE EXTERNAL TABLE IF NOT EXISTS default.refined_stocks (
    nome_acao STRING,
    abertura DOUBLE,
    fechamento DOUBLE,
    max DOUBLE,
    min DOUBLE,
    volume_negociado BIGINT,
    variacao_pct_dia DOUBLE,
    amplitude_dia DOUBLE,
    media_movel_7d DOUBLE,
    media_movel_14d DOUBLE,
    media_movel_30d DOUBLE,
    volatilidade_7d DOUBLE,
    lag_1d DOUBLE,
    lag_2d DOUBLE,
    lag_3d DOUBLE
)
PARTITIONED BY (data_pregao STRING)
STORED AS PARQUET
LOCATION 's3://818392673747-data-lake-bucket/refined/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 3. RECRIAR TABELA AGGREGATED_STOCKS_MONTHLY
CREATE EXTERNAL TABLE IF NOT EXISTS default.aggregated_stocks_monthly (
    nome_acao STRING,
    preco_medio_mensal DOUBLE,
    preco_minimo_mensal DOUBLE,
    preco_maximo_mensal DOUBLE,
    volume_total_mensal BIGINT,
    volume_medio_diario DOUBLE,
    variacao_media_diaria_pct DOUBLE,
    volatilidade_media_mensal DOUBLE,
    dias_negociacao BIGINT
)
PARTITIONED BY (mes_referencia STRING)
STORED AS PARQUET
LOCATION 's3://818392673747-data-lake-bucket/agg/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 4. ADICIONAR PARTIÇÕES AUTOMATICAMENTE
-- Essas queries vão descobrir automaticamente as partições no S3
MSCK REPAIR TABLE default.refined_stocks;
MSCK REPAIR TABLE default.aggregated_stocks_monthly;

-- 5. VERIFICAR PARTIÇÕES REGISTRADAS
SHOW PARTITIONS default.refined_stocks;
SHOW PARTITIONS default.aggregated_stocks_monthly;

-- 6. TESTAR CONSULTAS
SELECT COUNT(*) as total_registros FROM default.refined_stocks;
SELECT COUNT(*) as total_registros FROM default.aggregated_stocks_monthly;

-- 7. CONSULTA DE EXEMPLO
SELECT 
    data_pregao,
    nome_acao,
    fechamento,
    media_movel_7d
FROM default.refined_stocks
WHERE data_pregao >= '2025-07-01'
ORDER BY data_pregao DESC, nome_acao
LIMIT 10;
