from pyspark.sql import SparkSession
import yfinance as yf
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    cod = "B3SA3.SA"
    period = "1d"

    spark = SparkSession.builder.getOrCreate()

    logger.info(f"Fetching {cod} data for period: {period}")
    stock = yf.Ticker(cod)
    data = stock.history(period)

    spark_df = spark.createDataFrame(data)

    spark_df = spark_df.drop("Dividends", "Stock Splits")

    spark_df = (
        spark_df
        .withColumnRenamed("Open", "open")
        .withColumnRenamed("High", "high")
        .withColumnRenamed("Low", "low")
        .withColumnRenamed("Close", "close")
        .withColumnRenamed("Volume", "volume")
        .withColumnRenamed("Dividends", "dividends")
        .withColumnRenamed("Stock Splits", "stock_splits")
    )

    spark_df.show()

if __name__ == "__main__":
    main()