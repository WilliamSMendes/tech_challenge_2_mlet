import logging

import polars as pl
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    cod = "B3SA3.SA"
    period = "1d"

    logger.info(f"Fetching {cod} data for period: {period}")
    stock = yf.Ticker(cod)
    data = stock.history(period=period)

    df = pl.from_pandas(data.reset_index())
    df = df.drop([c for c in ["Dividends", "Stock Splits"] if c in df.columns])
    df = df.rename(
        {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits",
            "Date": "date",
        }
    )

    print(df)

if __name__ == "__main__":
    main()