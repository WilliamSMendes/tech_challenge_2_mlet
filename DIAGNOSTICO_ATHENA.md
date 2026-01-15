# Diagnostico e Correcao - Athena Queries

## PROBLEMA: Dados vazios no Athena

### Causa Mais Comum
Tabelas particionadas no Glue Catalog nao descobrem automaticamente as particoes criadas pelo Polars/Parquet.

---

## PASSO 1: DIAGNOSTICO

### 1.1 Verificar se as tabelas existem
```sql
SHOW TABLES IN default;
```

**Esperado:** `refined_stocks` e `aggregated_stocks_monthly`

---

### 1.2 Verificar schema das tabelas
```sql
DESCRIBE refined_stocks;
DESCRIBE aggregated_stocks_monthly;
```

**Esperado:** Ver as colunas + colunas de particao (data_pregao, nome_acao)

---

### 1.3 Verificar particoes registradas
```sql
SHOW PARTITIONS refined_stocks;
```

**Se retornar vazio ou erro:** As particoes nao foram registradas (problema principal!)

---

### 1.4 Verificar localizacao da tabela
```sql
SELECT "$path" FROM refined_stocks LIMIT 1;
```

ou

```sql
SHOW CREATE TABLE refined_stocks;
```

**Verifique:** Se o LOCATION esta correto (ex: `s3://818392673747-data-lake-bucket/refined/`)

---

## PASSO 2: SOLUCOES

### Solucao 1: Reparar Particoes (RECOMENDADO)
Execute este comando para descobrir automaticamente todas as particoes:

```sql
MSCK REPAIR TABLE refined_stocks;
```

**Esperado:** Mensagem indicando quantas particoes foram adicionadas

Depois teste:
```sql
SELECT COUNT(*) FROM refined_stocks;
```

---

### Solucao 2: Adicionar Particoes Manualmente (se MSCK nao funcionar)

Para cada data e acao, execute:

```sql
ALTER TABLE refined_stocks ADD IF NOT EXISTS
PARTITION (data_pregao='2025-07-15', nome_acao='itub4')
LOCATION 's3://818392673747-data-lake-bucket/refined/data_pregao=2025-07-15/nome_acao=itub4/';

ALTER TABLE refined_stocks ADD IF NOT EXISTS
PARTITION (data_pregao='2025-07-15', nome_acao='bbdc4')
LOCATION 's3://818392673747-data-lake-bucket/refined/data_pregao=2025-07-15/nome_acao=bbdc4/';

ALTER TABLE refined_stocks ADD IF NOT EXISTS
PARTITION (data_pregao='2025-07-15', nome_acao='bbas3')
LOCATION 's3://818392673747-data-lake-bucket/refined/data_pregao=2025-07-15/nome_acao=bbas3/';
```

**Substitua:**
- Bucket pelo seu bucket real
- Datas e acoes pelos valores reais dos seus dados

---

### Solucao 3: Recriar Tabela (ultima opcao)

```sql
DROP TABLE IF EXISTS refined_stocks;
```

Depois execute o Glue Job novamente para recriar a tabela.

---

## PASSO 3: VERIFICAR TABELA AGGREGATED

### 3.1 Testar tabela agregada (nao-particionada)
```sql
SELECT * FROM aggregated_stocks_monthly LIMIT 10;
```

**Se esta vazia tambem:** Verifique o location e se os arquivos parquet existem no S3

### 3.2 Reparar se necessario
```sql
MSCK REPAIR TABLE aggregated_stocks_monthly;
```

---

## PASSO 4: QUERIES DE TESTE

### Apos corrigir as particoes, teste:

```sql
-- Contar registros
SELECT COUNT(*) as total_registros FROM refined_stocks;

-- Ver ultimos 10 registros
SELECT * FROM refined_stocks 
ORDER BY data_pregao DESC 
LIMIT 10;

-- Contar por acao
SELECT 
    nome_acao,
    COUNT(*) as registros
FROM refined_stocks
GROUP BY nome_acao;

-- Dados agregados
SELECT * FROM aggregated_stocks_monthly
ORDER BY mes_referencia DESC, nome_acao;
```

---

## PASSO 5: VERIFICAR ARQUIVOS NO S3

Se mesmo assim nao funcionar, verifique se os arquivos existem:

Via AWS CLI:
```bash
# Listar arquivos refined
aws s3 ls s3://818392673747-data-lake-bucket/refined/ --recursive

# Listar arquivos agg
aws s3 ls s3://818392673747-data-lake-bucket/agg/ --recursive
```

Via Console AWS:
1. Acesse S3
2. Navegue ate seu bucket
3. Verifique as pastas `refined/` e `agg/`
4. Confirme que existem arquivos `.parquet`

---

## DIAGNOSTICO COMPLETO - CHECKLIST

- [ ] Tabelas existem no Glue Catalog
- [ ] Schema esta correto (colunas + particoes)
- [ ] LOCATION aponta para o path correto no S3
- [ ] Arquivos .parquet existem no S3
- [ ] Particoes foram registradas (MSCK REPAIR TABLE executado)
- [ ] Query SELECT retorna dados

---

## EXEMPLO COMPLETO DE CORRECAO

```sql
-- 1. Verificar tabelas
SHOW TABLES IN default;

-- 2. Reparar particoes
MSCK REPAIR TABLE refined_stocks;
MSCK REPAIR TABLE aggregated_stocks_monthly;

-- 3. Verificar particoes
SHOW PARTITIONS refined_stocks;

-- 4. Testar consulta
SELECT 
    data_pregao,
    nome_acao,
    fechamento,
    media_movel_7d
FROM refined_stocks
WHERE nome_acao = 'itub4'
ORDER BY data_pregao DESC
LIMIT 10;

-- 5. Verificar agregados
SELECT * FROM aggregated_stocks_monthly
ORDER BY mes_referencia DESC, nome_acao;
```

---

## RESOLUCAO ALTERNATIVA: USAR CRAWLER

Se o MSCK REPAIR nao funcionar, execute o Glue Crawler:

Via AWS CLI:
```bash
aws glue start-crawler --name refined_crawler
```

Via Console:
1. AWS Glue > Crawlers
2. Selecione `refined_crawler`
3. Clique em "Run"
4. Aguarde conclusao (1-2 minutos)
5. Teste novamente no Athena

---

## NOTAS IMPORTANTES

1. **MSCK REPAIR TABLE** funciona apenas se:
   - Os arquivos seguem o padrao de particionamento Hive: `coluna=valor/`
   - O Location da tabela esta correto
   - Voce tem permissoes S3 adequadas

2. **Particoes devem ser registradas** sempre que novos dados sao adicionados

3. **O codigo transform.py foi atualizado** para registrar particoes automaticamente nas proximas execucoes

4. **Para reprocessamento:**
   - Delete as tabelas: `DROP TABLE refined_stocks;`
   - Delete dados antigos no S3 (opcional)
   - Execute o pipeline completo novamente
