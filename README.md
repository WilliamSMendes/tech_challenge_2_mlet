# Tech Challenge 2 (MLET) — Pipeline B3 na AWS

Pipeline de dados (batch diário) para extrair cotações de ações da B3 via `yfinance`, ingerir os dados brutos em S3 (Parquet), acionar uma Lambda via evento do S3 para iniciar um Job no AWS Glue, e disponibilizar os dados para consulta via Athena.

## Visão geral

1. **EventBridge (agendado)** executa diariamente (19:00 BRT / 22:00 UTC) a Lambda de extração.
2. **Lambda `extract.py`** baixa dados de **D-1** (dia anterior) via yfinance e salva em **S3** (Parquet) na pasta `raw/` com particionamento Hive.
3. Após salvar, cria arquivo **`_SUCCESS`** que aciona **S3 Notification**.
4. **Lambda `trigger_glue.py`** verifica se não há job rodando e inicia o **Glue Job `transform_job`**.
5. **Glue `transform.py`** lê o bruto, faz transformações (feature engineering), grava dados em `refined/` e `agg/` em Parquet (formato Hive), e **cataloga automaticamente** via **MSCK REPAIR TABLE**.
6. **Athena** consulta os datasets no S3 (workgroup `etl_workgroup`) com partições automaticamente descobertas.

## Estrutura do repositório

```
functions/
	extract.py            # Lambda de extração (yfinance -> S3 raw)
	trigger_glue.py       # Lambda gatilho (S3 event -> start Glue job)
notebooks/
	01_yfinance_polars_exploration.ipynb
src/
	transform.py          # Script do Glue Job (raw -> refined/aggregated)
terraform/
	*.tf                  # Infra: S3, Lambda, Glue, Athena, Scheduler
requirements.txt        # Dependências para dev local/notebooks
```

## Dados extraídos

A Lambda de extração usa o `yfinance` com os tickers Blue Chips B3 (hardcoded no código):

- `ITUB4.SA` (Itaú Unibanco)
- `BBDC4.SA` (Bradesco)
- `BBAS3.SA` (Banco do Brasil)

**Período:** Apenas **D-1** (dia anterior). O pipeline roda diariamente às 19h BRT, extraindo dados de ontem.

**Modo Teste (dry_run):** A Lambda aceita `event.dry_run = true` para testar extração sem salvar no S3 (usado no smoke test do CI/CD).

## Layout no S3 (Formato Hive)

### RAW (Bronze Layer)

Arquivos parquet salvos com particionamento Hive:

```
s3://<DATA_LAKE_BUCKET>/raw/YYYY-MM-DD/data.parquet
s3://<DATA_LAKE_BUCKET>/raw/_SUCCESS  (trigger marker)
```

### REFINED (Silver Layer)

Dados tratados com features (particionamento Hive):

```
s3://<DATA_LAKE_BUCKET>/refined/data_pregao=YYYY-MM-DD/data.parquet
```

**Colunas:** `nome_acao`, `abertura`, `fechamento`, `max`, `min`, `volume_negociado`, `variacao_pct_dia`, `amplitude_dia`, `media_movel_7d`, `media_movel_14d`, `media_movel_30d`, `volatilidade_7d`, `lag_1d`, `lag_2d`, `lag_3d`

### AGG (Gold Layer)

Agregações mensais por ação (particionamento Hive):

```
s3://<DATA_LAKE_BUCKET>/agg/mes_referencia=YYYY-MM-DD/data.parquet
```

**Colunas:** `nome_acao`, `preco_medio_mensal`, `preco_minimo_mensal`, `preco_maximo_mensal`, `volume_total_mensal`, `volume_medio_diario`, `variacao_media_diaria_pct`, `volatilidade_media_mensal`, `dias_negociacao`

## Transformações (Glue)

O script do Glue (`src/transform.py`) usa **Polars** para processar:

### Feature Engineering:
- Renomeia colunas: `Open → abertura`, `Close → fechamento`, `Volume → volume_negociado`
- **Médias móveis:** 7, 14 e 30 dias por ticker
- **Lags temporais:** preços dos últimos 3 dias (lag_1d, lag_2d, lag_3d)
- **Volatilidade:** desvio padrão 7 dias
- **Métricas:** variação % diária, amplitude do dia

### Agregações Mensais:
- Preço médio, mínimo e máximo mensal
- Volume total e médio diário
- Variação média diária %
- Volatilidade média mensal
- Dias de negociação no mês

### Catalogação Automática:
- Cria/atualiza tabelas no **Glue Catalog** via boto3
- Executa **MSCK REPAIR TABLE** via Athena para descobrir todas as partições automaticamente
- Fallback para registro manual se MSCK falhar

## Infraestrutura (Terraform)

O Terraform provisiona:

### Storage:
- **S3 Data Lake Bucket:** raw/, refined/, agg/
- **S3 Source Code Bucket:** scripts do Glue Job
- **S3 Athena Results Bucket:** resultados de queries

### Compute:
- **Lambda Extract:** `b3_extract_function` (Python 3.10, 300s timeout, Layer: AWSSDKPandas)
- **Lambda Trigger:** `s3_trigger_glue_transform` (Python 3.10, 60s timeout)
- **Glue Job:** `transform_job` (2x G.1X workers, Polars + Python)

### Orquestração:
- **EventBridge Rule:** `daily_b3_etl_trigger` (cron `0 22 * * ? *` → 22:00 UTC / 19:00 BRT)
- **S3 Event Notification:** trigga Lambda quando detecta `raw/_SUCCESS`

### Analytics:
- **Athena Workgroup:** `etl_workgroup`
- **Glue Catalog:** database `default` com tabelas `refined_stocks` e `aggregated_stocks_monthly`

### Backend Terraform

O state usa backend S3 (configurado em `terraform/main.tf`). Garanta que o bucket de state exista e seja acessível ao role do deploy.

## Deploy (GitHub Actions)

O deploy roda em `push` na branch `main` via workflow `.github/workflows/deploy.yaml`.

O pipeline:

1. Prepara dependências da Lambda de extração em `functions/package/`.
2. Executa `terraform init`, `terraform fmt -check`, `terraform validate`, `terraform plan` e `terraform apply`.

### Permissões do role OIDC (GitHub Actions)

O workflow assume o role `github-actions-web-identity`.

Ele precisa permissões para criar/atualizar recursos que o Terraform gerencia. Exemplos mínimos (além do que você já usa para S3/Glue/IAM):

- `lambda:CreateFunction`, `lambda:UpdateFunctionCode`, `lambda:UpdateFunctionConfiguration`, `lambda:DeleteFunction`
- `lambda:AddPermission`/`lambda:RemovePermission` (S3 e EventBridge invocarem)
- `iam:PassRole` (Lambda e Glue precisam receber roles)
- `events:TagResource` (quando a regra do EventBridge tem `tags` no resource)

Obs.: este repo anexa essas permissões via Terraform no arquivo `terraform/athena.tf`.

## Consultas no Athena

O workgroup `etl_workgroup` salva resultados em:

```
s3://<ATHENA_RESULTS_BUCKET>/query_results/
```

### Tabelas Automáticas

As tabelas são **criadas e catalogadas automaticamente** pelo Glue Job via MSCK REPAIR TABLE:

- `default.refined_stocks` - Particionada por `data_pregao`
- `default.aggregated_stocks_monthly` - Particionada por `mes_referencia`

### Queries de Exemplo

```sql
-- Últimos 10 dias de uma ação
SELECT 
    data_pregao,
    nome_acao,
    fechamento,
    media_movel_7d,
    volatilidade_7d
FROM default.refined_stocks
WHERE nome_acao = 'itub4'
    AND data_pregao >= DATE_ADD('day', -10, CURRENT_DATE)
ORDER BY data_pregao DESC;

-- Agregação mensal de todas as ações
SELECT 
    mes_referencia,
    nome_acao,
    preco_medio_mensal,
    volume_total_mensal,
    dias_negociacao
FROM default.aggregated_stocks_monthly
ORDER BY mes_referencia DESC, nome_acao;
```

### Reparar Partições Manualmente (se necessário)

Se as partições não aparecerem automaticamente:

```sql
MSCK REPAIR TABLE default.refined_stocks;
MSCK REPAIR TABLE default.aggregated_stocks_monthly;
```

## Desenvolvimento local

Para explorar dados localmente (notebooks):

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

Abra o notebook em `notebooks/` e execute as células.

## Troubleshooting

- **Falha no `terraform fmt -check`**: rode `terraform -chdir=terraform fmt` e commite a formatação.
- **`AccessDenied` no `terraform apply` (Lambda/EventBridge)**: faltam permissões no role OIDC (`github-actions-web-identity`), especialmente `lambda:CreateFunction` e `events:TagResource`.
- **Não commitar `terraform/.terraform/`**: essa pasta é gerada por `terraform init` e deve ficar fora do versionamento.