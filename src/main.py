"""This module provides functionality for fetching and analyzing stock price data.

It includes capabilities to validate date inputs, fetch daily closing prices for given tickers,
calculate relative increases in stock prices, and determine the highest relative increase
over a specified period. It utilizes APIs for data retrieval and PySpark for data processing
and analysis. Functions are included for calculating the Compound Annual Growth Rate (CAGR)
and the final investment value based on relative stock price increases. The module supports
command-line arguments for specifying file locations, start dates, and end dates for analysis.
"""

import argparse
from datetime import datetime, date, timedelta
import requests

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max


API_KEY = "5MQqc0X4vZjap5jpjLxmJ1Lco_yBddG5"


def valid_date(s: str):
    """
    Validates if the input string represents a valid date in YYYY-MM-DD format.

    Args:
        s (str): The date string to validate.

    Returns:
        datetime.date: The validated date.

    Raises:
        argparse.ArgumentTypeError: If the input string is not a valid date.
    """
    try:
        timestamp = datetime.strptime(s, "%Y-%m-%d")
        return timestamp.date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def fetch_daily_closing_price(
    ticker: str = "AAPL", trading_date: date = date(2024, 2, 29)
):
    """
    Sends a request to an API to fetch the daily closing price of a given stock ticker and date.

    Args:
        ticker (str, optional): The stock ticker symbol. Defaults to "AAPL".
        date (date, optional): The date for the data request. Defaults to date(2024, 2, 29).

    Returns:
        dict: A dictionary containing the response data, which includes the symbol, closing price,
              or an error message in case of failure.
    """

    try:
        response = requests.get(
            url=f"https://api.polygon.io/v1/open-close/{ticker}/{trading_date}?adjusted=true&apiKey={API_KEY}"
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": f"Failed to fetch data: HTTP {response.status_code} {response.reason}"
            }
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {str(e)}"}


def parse_ticker_data(ticker: str, date: date):
    """
    Fetches the daily closing price for a given ticker and date, then formats the output.

    Args:
        ticker (str): The stock ticker symbol.
        date (date): The date for which to fetch the closing price.

    Returns:
        tuple: A tuple containing the ticker symbol, date, and closing price.
    """
    data = fetch_daily_closing_price(ticker=ticker, trading_date=date)
    print(data)
    return (data.get("symbol"), date, data.get("close"))


def calculate_relative_increase(stocks_price_data):
    """
    Calculates the relative increase of stock prices from their start to end dates.

    Args:
        stocks_price_data (DataFrame): A DataFrame containing stock ticker symbols, dates,
                                       and closing prices.

    Returns:
        DataFrame: A DataFrame containing the ticker symbols and their relative increase in prices.
    """
    start_end_dates = stocks_price_data.agg(
        min("date").alias("StartDate"), max("date").alias("EndDate")
    ).collect()

    # Storing the results into variables
    analysis_start_date = start_end_dates[0]["StartDate"]
    analysis_end_date = start_end_dates[0]["EndDate"]

    start_price_df = (
        stocks_price_data.filter(col("date") == analysis_start_date)
        .withColumn("start_price", col("closing_price"))
        .drop("closing_price")
    )
    print("######", analysis_start_date)
    start_price_df.show()

    end_price_df = (
        stocks_price_data.filter(col("date") == analysis_end_date)
        .withColumn("end_price", col("closing_price"))
        .drop("closing_price")
    )

    end_price_df.show()

    greatest_relative_increase = (
        start_price_df.join(end_price_df, "ticker", "inner")
        .withColumn(
            "relative_increse",
            ((col("end_price") - col("start_price")) / col("start_price")),
        )
        .orderBy("relative_increse")
    )

    greatest_relative_increase.show()

    return greatest_relative_increase


def calcualte_highest_relative_increase(df):
    """
    Calculates the relative increase of stock prices from their start to end dates.

    Args:
        stocks_price_data (DataFrame): A DataFrame containing stock ticker symbols, dates,
                                       and closing prices.

    Returns:
        DataFrame: A DataFrame containing the ticker symbols and their relative increase in prices.
    """
    max = df.agg({"relative_increase": "max"}).collect()[0][0]
    max_row = df.filter(df.relative_increase == max)
    max_row.select("*").show()


def calculate_investment_value(df):
    """
    Calculates the final value of an investment based on the relative increase of stock prices.

    Args:
        df (DataFrame): A DataFrame containing the relative increases of stock prices.

    No return value, but modifies the DataFrame to include the final value of an investment.
    """
    df = df.withColumn("final_value_at_period_end", col("relative_increase") * 10000)
    df = df.agg({"final_value_at_period_end": "sum"}).collect[0][0]


def calculate_monthly_cagr(df, analysis_start_date, analysis_end_date):
    """
    Calculates the Compound Annual Growth Rate (CAGR) on a monthly basis.

    Args:
        df (DataFrame): The DataFrame containing stock price data.
        start_date (date): The starting date of the period for the CAGR calculation.
        end_date (date): The ending date of the period for the CAGR calculation.

    Prints the maximum monthly CAGR and the associated data.
    """

    difference = relativedelta(analysis_start_date, analysis_end_date)
    total_months = difference.years * 12 + difference.months

    monthly_cagr = df.withColumn(
        "monthly_cagr", (col("start_price") / col("end_price")) ** (1 / total_months)
    )
    monthly_cagr.show()
    max_monthly_cagr = monthly_cagr.agg({"monthly_cagr": "max"}).collect()[0][0]

    print(max_monthly_cagr)

    max_score_row = monthly_cagr.filter(monthly_cagr.monthly_cagr == max_monthly_cagr)

    selected_values = max_score_row.select("*")
    selected_values.show()


def get_tickers(stocks_dataframe) -> list:
    """
    Extracts the ticker symbols from a DataFrame of stocks.

    Args:
        stocks (DataFrame): A DataFrame containing at least one column with stock ticker symbols.

    Returns:
        list: A list of ticker symbols.
    """

    tickers = [row["symbol"] for row in stocks_dataframe.select("symbol").collect()]
    return tickers


def get_price_data(tickers: list, analysis_start_date: date, analysis_end_date: date):
    """
    Fetches closing prices for a list of tickers between a start and end date.

    Args:
        tickers (list): A list of stock ticker symbols.
        start_date (date): The starting date for fetching data.
        end_date (date): The ending date for fetching data.

    Returns:
        list: A list of tuples containing ticker symbols, dates, and closing prices.
    """

    # Initialize the current date to start date
    current_date = analysis_start_date
    data = []
    # Loop through the date range
    while current_date <= analysis_end_date:
        print(current_date)
        # Increment the current date by one day
        current_date += timedelta(days=1)

        closing_prices = [
            parse_ticker_data(ticker=ticker, date=current_date) for ticker in tickers
        ]
        data = data + closing_prices

    print(data)
    return data


if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(
        description="Process user inputs; file location, start date and end date"
    )

    # Add the file-location argument (a required positional argument)
    parser.add_argument(
        "--file_location",
        help="Location of the stocks CSV file",
        default="data/stocks.csv",
    )

    # Add the start-date and end-date arguments (optional arguments but can be made required)
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

    # Accessing the arguments
    file_location = args.file_location
    start_date = args.start_date
    end_date = args.end_date

    print(file_location, start_date, end_date)

    # initialize the spark context
    spark = SparkSession.builder.appName("StratosTechnicalTest").getOrCreate()

    # read CSV file
    stocks = spark.read.csv(file_location, header=True)

    stocks_price_data = spark.createDataFrame(
        get_price_data(get_tickers(stocks), start_date, end_date),
        ["ticker", "date", "closing_price"],
    )
    stocks_price_data.show()
    stocks_price_data = calculate_relative_increase(stocks_price_data)
    
    
    calcualte_highest_relative_increase(stocks_price_data)
    calculate_monthly_cagr(stocks_price_data, start_date, end_date)
    calculate_investment_value(stocks_price_data)
