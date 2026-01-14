"""
transform.py - Transformação de dados de ações
Processa dados brutos (raw), aplica feature engineering e salva em:
- /refined: dados transformados particionados por data e nome da ação
- /agg: dados agregados mensalmente
"""
import sys
import polars as pl
import polars.selectors as cs
from awsglue.utils import getResolvedOptions

print("=" * 80)
print("INICIANDO TRANSFORMAÇÃO DE DADOS - BLUE CHIPS B3")
print("=" * 80)

# Lê argumentos passados pelo Glue Job
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'INPUT_PREFIX'])
    bucket_name = args['BUCKET_NAME']
    input_prefix = args['INPUT_PREFIX']
except Exception:
    # Fallback: compatibilidade com INPUT_KEY (versões antigas)
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'INPUT_KEY'])
    bucket_name = args['BUCKET_NAME']
    input_key = args['INPUT_KEY']
    input_prefix = input_key.rsplit('/', 1)[0] + '/'

input_path = f"s3://{bucket_name}/{input_prefix}"
print(f"\n📥 Lendo dados de: {input_path}\n")

# ============================================================================
# 1. LEITURA E LIMPEZA DOS DADOS RAW
# ============================================================================

# Lê dados raw em formato Parquet com Polars
df_raw = pl.read_parquet(input_path)
print(f"✓ Dados carregados: {df_raw.shape[0]:,} registros, {df_raw.shape[1]} colunas")
print(f"  Colunas: {', '.join(df_raw.columns)}\n")

# Normaliza tipos de dados e ordena
df_clean = df_raw.with_columns([
    pl.col("Ticker").cast(pl.Utf8, strict=False),
    pl.col("Date").cast(pl.Date, strict=False),
]).sort(["Ticker", "Date"])

# Remove registros com valores nulos nas colunas essenciais
df_clean = df_clean.filter(
    pl.col("Ticker").is_not_null() & 
    pl.col("Date").is_not_null() &
    pl.col("Close").is_not_null()
)

print(f"✓ Após limpeza: {df_clean.shape[0]:,} registros\n")

# ============================================================================
# 2. FEATURE ENGINEERING
# ============================================================================

print("🔧 Aplicando transformações e criando features...\n")

df_refined = df_clean.with_columns([
    # Renomeia e padroniza colunas
    pl.col("Date").alias("data_pregao"),
    pl.col("Ticker").str.replace(".SA", "").str.to_lowercase().alias("nome_acao"),
    pl.col("Open").alias("abertura"),
    pl.col("Close").alias("fechamento"),
    pl.col("High").alias("max"),
    pl.col("Low").alias("min"),
    pl.col("Volume").alias("volume_negociado"),
    
    # Médias móveis (7, 14 e 30 dias)
    pl.col("Close").rolling_mean(window_size=7).over("Ticker").alias("media_movel_7d"),
    pl.col("Close").rolling_mean(window_size=14).over("Ticker").alias("media_movel_14d"),
    pl.col("Close").rolling_mean(window_size=30).over("Ticker").alias("media_movel_30d"),
    
    # Lags (1, 2 e 3 dias anteriores)
    pl.col("Close").shift(1).over("Ticker").alias("lag_1d"),
    pl.col("Close").shift(2).over("Ticker").alias("lag_2d"),
    pl.col("Close").shift(3).over("Ticker").alias("lag_3d"),
    
    # Variação percentual diária
    ((pl.col("Close") - pl.col("Open")) / pl.col("Open") * 100).alias("variacao_pct_dia"),
    
    # Amplitude do dia (diferença entre máxima e mínima)
    (pl.col("High") - pl.col("Low")).alias("amplitude_dia"),
    
    # Volatilidade (desvio padrão móvel de 7 dias)
    pl.col("Close").rolling_std(window_size=7).over("Ticker").alias("volatilidade_7d"),
])

# Remove registros com nulls (geralmente dos primeiros dias por causa de lags/médias)
df_refined = df_refined.drop_nulls()

# Arredonda valores float para 2 casas decimais
df_refined = df_refined.with_columns(cs.float().round(2))

# Seleciona colunas finais na ordem desejada
df_final = df_refined.select([
    "data_pregao",
    "nome_acao",
    "abertura",
    "fechamento",
    "max",
    "min",
    "volume_negociado",
    "variacao_pct_dia",
    "amplitude_dia",
    "media_movel_7d",
    "media_movel_14d",
    "media_movel_30d",
    "volatilidade_7d",
    "lag_1d",
    "lag_2d",
    "lag_3d",
])

print(f"✓ Features criadas: {df_final.shape[1]} colunas")
print(f"✓ Registros finais: {df_final.shape[0]:,}\n")

# ============================================================================
# 3. SALVAR DADOS REFINED (PARTICIONADOS POR DATA E NOME DA AÇÃO)
# ============================================================================

output_path_refined = f"s3://{bucket_name}/refined/"
print(f"💾 Salvando dados REFINED em: {output_path_refined}")
print(f"   Particionamento: data_pregao + nome_acao\n")

df_final.write_parquet(
    output_path_refined,
    use_pyarrow=True,
    partition_by=["data_pregao", "nome_acao"],
    compression="snappy"
)

print("✓ Dados refined salvos com sucesso!\n")

# ============================================================================
# 4. DADOS AGREGADOS MENSAIS
# ============================================================================

print("📊 Gerando agregações mensais...\n")

df_agregado = df_refined.group_by([
    "nome_acao",
    pl.col("data_pregao").dt.truncate("1mo").alias("mes_referencia")
]).agg([
    # Agregações de preço
    pl.col("fechamento").mean().alias("preco_medio_mensal"),
    pl.col("fechamento").min().alias("preco_minimo_mensal"),
    pl.col("fechamento").max().alias("preco_maximo_mensal"),
    
    # Agregações de volume
    pl.col("volume_negociado").sum().alias("volume_total_mensal"),
    pl.col("volume_negociado").mean().alias("volume_medio_diario"),
    
    # Estatísticas de variação
    pl.col("variacao_pct_dia").mean().alias("variacao_media_diaria_pct"),
    pl.col("volatilidade_7d").mean().alias("volatilidade_media_mensal"),
    
    # Contagem de dias de negociação
    pl.col("data_pregao").n_unique().alias("dias_negociacao"),
]).sort(["nome_acao", "mes_referencia"])

# Arredonda valores
df_agregado = df_agregado.with_columns(cs.float().round(2))

print(f"✓ Agregações geradas: {df_agregado.shape[0]:,} registros mensais\n")

# Salva dados agregados
output_path_agg = f"s3://{bucket_name}/agg/"
print(f"💾 Salvando dados AGREGADOS em: {output_path_agg}\n")

df_agregado.write_parquet(
    output_path_agg,
    use_pyarrow=True,
    compression="snappy"
)

print("✓ Dados agregados salvos com sucesso!\n")

# ============================================================================
# RESUMO FINAL
# ============================================================================

print("=" * 80)
print("✅ TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
print("=" * 80)
print(f"📊 Estatísticas finais:")
print(f"   • Registros refined:  {df_final.shape[0]:,}")
print(f"   • Registros agregados: {df_agregado.shape[0]:,}")
print(f"   • Ações processadas:  {df_final['nome_acao'].n_unique()}")
print(f"   • Features criadas:   {df_final.shape[1]}")
print("=" * 80)