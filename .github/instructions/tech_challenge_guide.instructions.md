O problema
Pipeline Batch Bovespa: ingestão e arquitetura de dados
Construa um pipeline de dados completo para extrair, processar e
analisar dados de ações ou índices da B3, utilizando AWS S3, Glue, Lambda e
Athena. Você pode extrair esses dados através de bibliotecas como yfinance ou
realizar scrapping de sites que contenham esses dados (Exemplo: ibovespa).
Para esse desafio, sua entrega deve conter os seguintes requisitos:
● Requisito 1: scrap de dados de ações ou índices da B3
(granularidade diária).
● Requisito 2: os dados brutos devem ser ingeridos no s3 em formato
parquet com partição diária.
● Requisito 3: o bucket deve acionar uma lambda, que por sua vez irá
chamar o job de ETL no glue.
● Requisito 4: a lambda pode ser em qualquer linguagem. Ela apenas
deverá iniciar o job Glue.
● Requisito 5: o job Glue pode ser feito no modo visual ou via código.
Este job deve conter as seguintes transformações obrigatórias:
o A: agrupamento numérico, sumarização, contagem ou soma.
o B: renomear duas colunas existentes além das de agrupamento.
o C: realizar um cálculo com base na data, como por exemplo
média móvel, diferença entre períodos, valores extremos no
período etc.
● Requisito 6: os dados refinados no job glue devem ser salvos no
formato parquet em uma pasta chamada refined, particionado por
data e pelo nome ou código da ação/índice.
● Requisito 7: o job Glue deve automaticamente catalogar o dado no
Glue Catalog e criar uma tabela no banco de dados (pode ser o
default).
● Requisito 8: os dados devem estar disponíveis e serem consultados
usando SQL através do Athena