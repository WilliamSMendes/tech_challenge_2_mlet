# Tech Challenge 2 (MLET) — Pipeline B3 na AWS

Pipeline de dados (batch diário) para extrair cotações de ações da B3 via `yfinance`, ingerir os dados brutos em S3 (Parquet), acionar uma Lambda via evento do S3 para iniciar um Job no AWS Glue, e disponibilizar os dados para consulta via Athena.

## Visão geral

1. **EventBridge (agendado)** executa diariamente a Lambda de extração.
2. **Lambda `extract.py`** baixa dados históricos (últimos 6 meses) e salva o bruto em **S3** (Parquet) na pasta `raw/` com partição por data de ingestão.
3. **S3 Notification** (prefixo `raw/`, sufixo `.parquet`) aciona a Lambda de gatilho.
4. **Lambda `trigger_glue.py`** inicia o **Glue Job `transform_job`**.
5. **Glue `transform.py`** lê o bruto, faz transformações, e grava dados em `refined/` e `aggregated/` em Parquet.
6. **Athena** consulta os datasets no S3 (workgroup `etl_workgroup`).

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

A Lambda de extração usa o `yfinance` com os tickers (hardcoded no código):

- `TOTS3.SA`
- `LWSA3.SA`
- `POSI3.SA`
- `INTB3.SA`
- `WEGE3.SA`

Período: do dia **D-1** até **6 meses antes**.

## Layout no S3

### RAW

Arquivo parquet salvo em:

```
s3://<DATA_LAKE_BUCKET>/raw/ingestion_date=YYYY-MM-DD/stocks_data.parquet
```

### REFINED

Dados tratados e com features em:

```
s3://<DATA_LAKE_BUCKET>/refined/
```

Particionamento: `data_pregao`.

### AGGREGATED

Agregações mensais (volume total e preço médio mensal):

```
s3://<DATA_LAKE_BUCKET>/aggregated/
```

## Transformações (Glue)

O script do Glue (`src/transform.py`) aplica:

- Renomeia colunas (ex.: `Open -> abertura`, `Close -> fechamento`, `Volume -> volume_negociado`).
- Cálculo baseado em data/ordem temporal: **média móvel 7 dias** e **lags** (1d/2d/3d) por ticker.
- Agregação: cálculo mensal com `sum(volume)` e `mean(fechamento)`.

## Infraestrutura (Terraform)

O Terraform cria, entre outros:

- Buckets S3: data lake, source code (scripts), resultados do Athena
- 2 Lambdas: `b3_extract_function` e `s3_trigger_glue_transform`
- Glue Job: `transform_job`
- EventBridge Rule: `daily_b3_etl_trigger` (cron `0 9 * * ? *` → 09:00 UTC)
- Athena Workgroup: `etl_workgroup`

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

O workgroup criado é `etl_workgroup` e os resultados vão para o bucket `athena_results_bucket` em:

```
s3://<ATHENA_RESULTS_BUCKET>/query_results/
```

Para consultar `refined/` e `aggregated/`, crie tabelas no Glue Data Catalog (via Glue Crawler ou SQL no Athena).

Exemplo (ajuste o `LOCATION` para o seu bucket):

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS refined_stocks (
	nome_acao string,
	abertura double,
	fechamento double,
	max double,
	min double,
	volume_negociado bigint,
	media_movel_7d double,
	lag_1d double,
	lag_2d double,
	lag_3d double
)
PARTITIONED BY (data_pregao date)
STORED AS PARQUET
LOCATION 's3://<DATA_LAKE_BUCKET>/refined/';
```

Depois, rode `MSCK REPAIR TABLE refined_stocks;` para carregar partições.

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