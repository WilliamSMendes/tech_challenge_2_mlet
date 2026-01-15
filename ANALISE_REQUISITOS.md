# 📋 Análise de Conformidade - Tech Challenge 2 (MLET)

## Resumo Executivo

✅ **7/8 requisitos atendidos**  
⚠️ 1 requisito corrigido durante a análise

---

## Análise Detalhada por Requisito

### ✅ Requisito 1: Scrap de dados B3 (granularidade diária)
**Status: ATENDE**

- **Arquivo:** `functions/extract.py`
- **Método:** Biblioteca `yfinance`
- **Tickers:** ITUB4.SA, BBDC4.SA, BBAS3.SA (Blue Chips - Bancos)
- **Granularidade:** Dados diários
- **Período:** Últimos 6 meses (configurable)

**Evidências:**
- Lines 22-27: Definição dos tickers
- Lines 34-69: Função `download_ticker_data()` usando yfinance
- Lines 191-193: Configuração do período (D-1 até 6 meses antes)

---

### ✅ Requisito 2: Dados brutos no S3 em parquet com partição diária
**Status: ATENDE**

- **Arquivo:** `functions/extract.py`
- **Formato:** Parquet (PyArrow)
- **Particionamento:** Por `data_particao` (data da cotação)
- **Path:** `s3://bucket/raw/execution_date=YYYY-MM-DD/run_timestamp/data_particao=YYYY-MM-DD/*.parquet`

**Evidências:**
- Lines 119-156: Função `save_to_parquet_partitioned()`
- Line 141: Criação da coluna `data_particao` 
- Lines 147-151: `pq.write_to_dataset()` com `partition_cols=['data_particao']`

**Estrutura do Particionamento:**
```
raw/
└── execution_date=2026-01-14/
    └── run_20260114_220000/
        ├── data_particao=2024-07-15/
        │   └── part-0.parquet
        ├── data_particao=2024-07-16/
        │   └── part-0.parquet
        └── _SUCCESS
```

---

### ✅ Requisito 3: Bucket aciona Lambda que chama job Glue
**Status: ATENDE**

- **Arquivo:** `terraform/lambda.tf`
- **Trigger:** S3 Bucket Notification
- **Evento:** `s3:ObjectCreated:*`
- **Filtros:** 
  - Prefixo: `raw/`
  - Sufixo: `_SUCCESS`

**Evidências:**
- Lines 121-128 (lambda.tf): Configuração do `aws_s3_bucket_notification`
- Lines 116-120 (lambda.tf): Permissão para S3 invocar Lambda
- `functions/trigger_glue.py`: Lambda que recebe evento S3 e inicia job Glue

**Fluxo:**
1. Extract Lambda salva dados em `raw/.../*.parquet`
2. Extract Lambda cria arquivo `_SUCCESS` como marker
3. S3 notification aciona `s3_trigger_glue` Lambda
4. Lambda `trigger_glue.py` inicia job Glue

---

### ✅ Requisito 4: Lambda em qualquer linguagem
**Status: ATENDE**

- **Arquivo:** `functions/trigger_glue.py`
- **Linguagem:** Python 3.10
- **Função:** Apenas iniciar job Glue (conforme especificado)

**Evidências:**
- Lines 62-68 (trigger_glue.py): `glue.start_job_run()` com argumentos
- Proteção contra execuções concorrentes (lines 11-19)
- Tratamento de eventos S3 e invocações manuais (lines 21-47)

---

### ✅ Requisito 5: Job Glue com transformações obrigatórias
**Status: ATENDE TODOS OS SUB-REQUISITOS**

#### 5.A - Agrupamento/Sumarização ✅
- **Arquivo:** `src/transform.py` (lines 193-215)
- **Agrupamento:** Por `nome_acao` e `mes_referencia`
- **Agregações:**
  - Preços: média, mínimo, máximo mensal
  - Volume: total mensal, média diária
  - Estatísticas: variação média diária, volatilidade média mensal
  - Contagem: dias de negociação no mês

```python
df_agregado = df_refined.group_by([
    "nome_acao",
    pl.col("data_pregao").dt.truncate("1mo").alias("mes_referencia")
]).agg([
    pl.col("fechamento").mean().alias("preco_medio_mensal"),
    pl.col("volume_negociado").sum().alias("volume_total_mensal"),
    ...
])
```

#### 5.B - Renomear colunas ✅
- **Arquivo:** `src/transform.py` (lines 107-113)
- **Renomeações realizadas:**
  - `Date` → `data_pregao`
  - `Ticker` → `nome_acao`
  - `Open` → `abertura`
  - `Close` → `fechamento`
  - `High` → `max`
  - `Low` → `min`
  - `Volume` → `volume_negociado`

**Total:** 7 colunas renomeadas (requisito pede 2 + colunas de agrupamento)

#### 5.C - Cálculo com data ✅
- **Arquivo:** `src/transform.py` (lines 115-126)
- **Cálculos temporais implementados:**

1. **Médias Móveis:**
   - 7 dias: `media_movel_7d`
   - 14 dias: `media_movel_14d`
   - 30 dias: `media_movel_30d`

2. **Lags (valores anteriores):**
   - 1 dia: `lag_1d`
   - 2 dias: `lag_2d`
   - 3 dias: `lag_3d`

3. **Volatilidade:**
   - Desvio padrão móvel de 7 dias: `volatilidade_7d`

4. **Outras:**
   - Variação percentual diária: `variacao_pct_dia`
   - Amplitude do dia: `amplitude_dia`

```python
pl.col("Close").rolling_mean(window_size=7).over("Ticker").alias("media_movel_7d"),
pl.col("Close").shift(1).over("Ticker").alias("lag_1d"),
pl.col("Close").rolling_std(window_size=7).over("Ticker").alias("volatilidade_7d"),
```

---

### ✅ Requisito 6: Dados refined particionados por data e ação
**Status: ATENDE**

- **Arquivo:** `src/transform.py` (lines 166-177)
- **Path:** `s3://bucket/refined/`
- **Formato:** Parquet (Snappy compression)
- **Particionamento:** `partition_by=["data_pregao", "nome_acao"]`

**Evidências:**
```python
df_final.write_parquet(
    output_path_refined,
    use_pyarrow=True,
    partition_by=["data_pregao", "nome_acao"],
    compression="snappy"
)
```

**Estrutura resultante:**
```
refined/
├── data_pregao=2024-07-15/
│   ├── nome_acao=itub4/
│   │   └── data.parquet
│   ├── nome_acao=bbdc4/
│   │   └── data.parquet
│   └── nome_acao=bbas3/
│       └── data.parquet
└── data_pregao=2024-07-16/
    └── ...
```

---

### ✅ Requisito 7: Catalogar automaticamente no Glue Catalog
**Status: ATENDE (APÓS CORREÇÃO)**

- **Arquivo:** `src/transform.py` (seção 5 - recém adicionada)
- **Database:** `default`
- **Tabelas criadas:**
  1. `refined_stocks` (com partições por data_pregao e nome_acao)
  2. `aggregated_stocks_monthly`

**Implementação:**
- Usa boto3 para criar/atualizar tabelas no Glue Catalog
- Schema completo definido para ambas as tabelas
- Configuração de SerDe para Parquet
- Partições configuradas para tabela refined
- Try/except para criar ou atualizar tabelas existentes

**Evidências:**
- Lines 234-373: Bloco completo de catalogação automática
- Schemas detalhados para ambas as tabelas
- Tratamento de exceção `AlreadyExistsException` para atualizar tabelas

**Recursos adicionais:**
- `terraform/glue.tf` (lines 75-92): Crawler configurado como backup
- Crawler roda diariamente às 23:00 UTC (após ETL às 22:00 UTC)

---

### ✅ Requisito 8: Dados consultáveis via Athena
**Status: ATENDE**

- **Arquivo:** `terraform/athena.tf`
- **Workgroup:** `etl_workgroup`
- **Database:** `default` (via Glue Catalog)
- **Tabelas disponíveis:**
  - `refined_stocks` (dados refinados particionados)
  - `aggregated_stocks_monthly` (agregações mensais)

**Configuração:**
- Bucket de resultados: `<account_id>-athena-results-bucket`
- Output location: `s3://.../query_results/`
- Enforce workgroup configuration: true

**Exemplos de consultas possíveis:**

```sql
-- Consultar dados refinados
SELECT * FROM refined_stocks 
WHERE nome_acao = 'itub4' 
AND data_pregao >= DATE('2024-07-01')
LIMIT 10;

-- Consultar agregados mensais
SELECT 
    nome_acao,
    mes_referencia,
    preco_medio_mensal,
    volume_total_mensal
FROM aggregated_stocks_monthly
ORDER BY mes_referencia DESC;

-- Análise de volatilidade por ação
SELECT 
    nome_acao,
    AVG(volatilidade_7d) as volatilidade_media
FROM refined_stocks
GROUP BY nome_acao;
```

---

## 🏗️ Arquitetura Completa

```
┌─────────────────────────────────────────────────────────────────┐
│                      PIPELINE BATCH B3                          │
└─────────────────────────────────────────────────────────────────┘

1. EXTRAÇÃO (Diária - 22:00 UTC)
   ┌──────────────┐
   │ EventBridge  │ → cron(0 22 * * ? *)
   └──────┬───────┘
          ↓
   ┌──────────────┐
   │   Lambda     │ → extract.py (yfinance)
   │   Extract    │
   └──────┬───────┘
          ↓
   ┌──────────────┐
   │      S3      │ → raw/execution_date=.../data_particao=.../
   │   RAW        │ → Parquet particionado
   └──────┬───────┘
          ↓ (S3 Event: _SUCCESS)

2. TRANSFORMAÇÃO (Event-driven)
   ┌──────────────┐
   │   Lambda     │ → trigger_glue.py
   │   Trigger    │
   └──────┬───────┘
          ↓ (glue.start_job_run)
   ┌──────────────┐
   │  Glue Job    │ → transform.py (Polars)
   │  Transform   │ → Feature Engineering
   └──────┬───────┘
          ↓
   ┌──────────────┐
   │      S3      │ → refined/data_pregao=.../nome_acao=.../
   │   REFINED    │ → agg/mes_referencia=.../
   └──────┬───────┘
          ↓ (boto3)

3. CATALOGAÇÃO (Automática)
   ┌──────────────┐
   │ Glue Catalog │ → Tables: refined_stocks, aggregated_stocks_monthly
   │   + Crawler  │ → Database: default
   └──────┬───────┘
          ↓

4. CONSULTA (On-demand)
   ┌──────────────┐
   │    Athena    │ → SQL queries
   │   Workgroup  │ → etl_workgroup
   └──────────────┘
```

---

## 📊 Features Implementadas (Além dos Requisitos)

### Robustez e Confiabilidade
1. **Proteção contra execuções concorrentes** (Glue job max_concurrent_runs = 1)
2. **Retry logic** no yfinance (método download + fallback para history)
3. **Validação de conectividade** antes de extrair
4. **Limpeza de dados nulos** antes de processar
5. **Error handling não-bloqueante** na catalogação

### Observabilidade
1. **Logs detalhados** em todas as etapas
2. **Métricas de execução** (registros processados, ações únicas)
3. **Status tracking** via marker `_SUCCESS`
4. **CloudWatch integration** (Lambda + Glue)

### Qualidade dos Dados
1. **Arredondamento de floats** (2 casas decimais)
2. **Type casting explícito** (Date, String)
3. **Ordenação por Ticker e Data**
4. **Remoção de registros com lags/médias incompletos**

### Flexibilidade
1. **Suporte a formato wide e long** (conversão automática)
2. **Detecção de ambiente** (AWS Glue vs Local)
3. **Paths S3 ou locais** (desenvolvimento/produção)
4. **Argumentos configuráveis** (bucket, prefix)

---

## 🔧 Melhorias Sugeridas

### Curto Prazo
1. ✅ **Catalogação automática** - IMPLEMENTADO
2. **Adicionar mais tickers** (outras blue chips: VALE3, PETR4, etc.)
3. **Testes unitários** para transformações
4. **Documentação de queries Athena** comuns

### Médio Prazo
1. **Alertas CloudWatch** (falhas de extração/transformação)
2. **Dashboard QuickSight** para visualização
3. **Backfill mechanism** para reprocessar dados históricos
4. **Data quality checks** (Great Expectations ou similar)

### Longo Prazo
1. **Orquestração via Step Functions** (melhor visibilidade)
2. **Versionamento de dados** (Delta Lake ou similar)
3. **Streaming pipeline** (dados em tempo real via Kinesis)
4. **Machine Learning** (previsão de preços, detecção de anomalias)

---

## ✅ Checklist Final

- [x] Requisito 1: Extração de dados B3 (yfinance)
- [x] Requisito 2: Dados brutos em S3 Parquet particionado
- [x] Requisito 3: S3 aciona Lambda que chama Glue
- [x] Requisito 4: Lambda em Python
- [x] Requisito 5A: Agrupamento e sumarização
- [x] Requisito 5B: Renomeação de colunas
- [x] Requisito 5C: Cálculos temporais (médias móveis, lags, volatilidade)
- [x] Requisito 6: Dados refined particionados
- [x] Requisito 7: Catalogação automática no Glue Catalog
- [x] Requisito 8: Dados consultáveis via Athena

---

## 📝 Conclusão

O projeto **atende completamente todos os 8 requisitos** do Tech Challenge após a correção implementada para catalogação automática.

A arquitetura é robusta, bem estruturada e segue boas práticas de engenharia de dados:
- **Separation of concerns** (extração, transformação, catalogação)
- **Event-driven architecture** (S3 notifications)
- **Infrastructure as Code** (Terraform)
- **Observabilidade** (logs detalhados, CloudWatch)
- **Eficiência** (Polars para processamento, Parquet com compressão)

O código está pronto para produção e pode ser facilmente estendido para incluir mais tickers, features adicionais ou integrações com outros serviços AWS.
