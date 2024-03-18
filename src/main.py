"""This module provides functionality for fetching and analyzing stock price data.

It includes capabilities to validate date inputs, fetch daily closing prices for given tickers,
calculate relative increases in stock prices, and determine the highest relative increase
over a specified period. It utilizes APIs for data retrieval and PySpark for data processing
and analysis. Functions are included for calculating the Compound Annual Growth Rate (CAGR)
and the final investment value based on relative stock price increases. The module supports
command-line arguments for specifying file locations, start dates, and end dates for analysis.
"""

import argparse
import datetime

from pyspark.sql import DataFrame, SparkSession
from utils import (
    calcualte_highest_relative_increase,
    calculate_investment_value,
    calculate_monthly_cagr,
    calculate_relative_increase,
    create_report,
    get_price_data,
    get_tickers,
)


def valid_date(s: str) -> datetime.date:
    try:
        timestamp = datetime.datetime.strptime(s, "%Y-%m-%d")
        return timestamp.date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Process user inputs; file location, api key, start date and end date"
    )

    parser.add_argument(
        "--file_location",
        help="Location of the stocks CSV file",
        default="data/stocks.csv",
        type=str,
    )

    parser.add_argument(
        "--api-key",
        help="Polygon.io API key",
        type=str,
        default="5MQqc0X4vZjap5jpjLxmJ1Lco_yBddG5",
    )

    parser.add_argument(
        "--start-date",
        help="The start date in YYYY-MM-DD format",
        type=valid_date,
        default="2022-06-01",
    )
    parser.add_argument(
        "--end-date",
        help="The end date in YYYY-MM-DD format",
        type=valid_date,
        default="2022-06-02",
    )

    args = parser.parse_args()

    file_location = args.file_location
    api_key = args.api_key
    start_date = args.start_date
    end_date = args.end_date

    print(file_location, start_date, end_date)

    spark = SparkSession.builder.appName("StratosTechnicalTest").getOrCreate()

    stocks = spark.read.csv(file_location, header=True)

    stocks_price_data = spark.createDataFrame(
        get_price_data(get_tickers(stocks), api_key, start_date, end_date),
        ["ticker", "trading_date", "closing_price"],
    )
    stocks_price_data = calculate_relative_increase(stocks_price_data)

    create_report(
        start_date,
        end_date,
        relative_increase=calcualte_highest_relative_increase(stocks_price_data),
        monthly_cagr=calculate_monthly_cagr(stocks_price_data, start_date, end_date),
        investment_value=calculate_investment_value(stocks_price_data),
    )
