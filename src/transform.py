# src/transform.py
import sys
import boto3
import polars as pl
import polars.selectors as cs
from awsglue.utils import getResolvedOptions

# Le argumentos passados pelo Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'INPUT_KEY'])
bucket_name = args['BUCKET_NAME']
input_key = args['INPUT_KEY']

input_path = f"s3://{bucket_name}/{input_key}"
print(f"Lendo dados de: {input_path}")

# Le o arquivo RAW com Polars
df_raw = pl.read_parquet(input_path)

# Ordena os dados por Ticker e Date para garantir consistência nos cálculos
df_clean = df_raw.sort(["Ticker", "Date"])

# Feature Engineering
df_refined = df_clean.with_columns([
    pl.col("Date").cast(pl.Date).alias("data_pregao"),
    pl.col("Ticker").str.to_lowercase().alias("nome_acao"),
    pl.col("Open").alias("abertura"),
    pl.col("Close").alias("fechamento"),
    pl.col("High").alias("max"),
    pl.col("Low").alias("min"),
    pl.col("Volume").alias("volume_negociado"),
    
    # Médias e Lags
    pl.col("Close").rolling_mean(window_size=7).over("Ticker").alias("media_movel_7d"),
    pl.col("Close").shift(1).over("Ticker").alias("lag_1d"),
    pl.col("Close").shift(2).over("Ticker").alias("lag_2d"),
    pl.col("Close").shift(3).over("Ticker").alias("lag_3d")
])

# Limpeza e Arredondamento
df_final = df_refined.drop_nulls().with_columns(cs.float().round(2))

df_final = df_final.select([
    "data_pregao", "nome_acao", "abertura", "fechamento", "max", "min",
    "volume_negociado", "media_movel_7d", "lag_1d", "lag_2d", "lag_3d"
])


# Salva particionado para o Athena ler otimizado
output_path_refined = f"s3://{bucket_name}/refined/"
df_final.write_parquet(
    output_path_refined,
    use_pyarrow=True,
    partition_by=["data_pregao"]
)

print("Dados refinados salvos com sucesso.")

# Dados Agregados Mensais
df_agregado = df_refined.group_by(["Ticker", pl.col("data_pregao").dt.truncate("1mo")]).agg([
    pl.col("volume_negociado").sum().alias("volume_mensal_total"),
    pl.col("fechamento").mean().alias("preco_medio_mensal").round(2)
]).sort(["Ticker", "data_pregao"])

output_path_agregado = f"s3://{bucket_name}/aggregated/"
df_agregado.write_parquet(output_path_agregado)

print("Dados agregados salvos com sucesso.")