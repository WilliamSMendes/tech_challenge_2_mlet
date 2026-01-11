import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler do S3 (Raw)
# Assumindo que o extract.py salvou em parquet
dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://SEU_BUCKET_DATALAKE/raw/"], "recurse": True},
    transformation_ctx="dyf"
)

# Converter para DataFrame Spark (similar ao Polars/Pandas)
df = dyf.toDF()

# Aplica as transformações do Requisito 5 aqui usando PySpark
# Ex: Rename
df = df.withColumnRenamed("Close", "fechamento") \
       .withColumnRenamed("Volume", "volume_negociado")

# Ex: Salvar na Refined particionado (Requisito 6)
save_path = "s3://SEU_BUCKET_DATALAKE/refined/"
df.write.mode("overwrite").partitionBy("data_pregao", "Ticker").parquet(save_path)

job.commit()