from datetime import datetime, date, timedelta
from typing import Any
import requests
import logging


from ratelimit import limits, sleep_and_retry
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, min, max, sum, first


def calcualte_highest_relative_increase(df):

    max_value = df.agg({"relative_increase": "max"}).collect()[0][0]
    max_row = df.filter(df.relative_increase == max_value)
    return {
        "ticker": max_row.collect()[0]["ticker"],
        "relative_increase": max_value,
    }


def calculate_investment_value(df):

    df = df.withColumn(
        "final_value_at_period_end",
        (col("relative_increase").cast("double") + 1) * 10000,
    )

    sum_df = df.agg(sum("final_value_at_period_end").alias("TotalValueOfInvestment"))

    total_sum_value = sum_df.collect()[0]["TotalValueOfInvestment"]
    return total_sum_value


@sleep_and_retry
@limits(calls=80, period=2)
def fetch_daily_closing_price(
    ticker: str = "AAPL", trading_date: date = date(2024, 2, 29), api_key: str = ""
) -> Any | dict[str, str]:

    try:
        response = requests.get(
            url=f"https://api.polygon.io/v1/open-close/{ticker}/{trading_date}?adjusted=true&apiKey={api_key}"
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "error": f"Failed to fetch data: HTTP {response.status_code} {response.reason}"
            }
    except requests.exceptions.RequestException as e:
        return {"error": f"Request failed: {str(e)}"}


def parse_ticker_data(ticker: str, date: date, api_key: str) -> set:

    data = fetch_daily_closing_price(ticker=ticker, trading_date=date, api_key=api_key)
    return (data.get("symbol"), date, data.get("close"))


def calculate_relative_increase(stocks_price_data):
    start_end_dates = stocks_price_data.agg(
        min("trading_date").alias("StartDate"), max("trading_date").alias("EndDate")
    ).collect()

    # Storing the results into variables
    analysis_start_date = start_end_dates[0]["StartDate"]
    analysis_end_date = start_end_dates[0]["EndDate"]

    start_price_df = (
        stocks_price_data.filter(col("trading_date") == analysis_start_date)
        .withColumn("start_price", col("closing_price"))
        .drop("closing_price")
    )
    end_price_df = (
        stocks_price_data.filter(col("trading_date") == analysis_end_date)
        .withColumn("end_price", col("closing_price"))
        .drop("closing_price")
    )

    greatest_relative_increase = (
        start_price_df.join(end_price_df, "ticker", "inner")
        .withColumn(
            "relative_increase",
            (col("end_price") - col("start_price")) / col("start_price"),
        )
        .orderBy("relative_increase")
    )

    return greatest_relative_increase


def get_tickers(stocks_dataframe) -> list:
    tickers = [row["symbol"] for row in stocks_dataframe.select("symbol").collect()]
    return tickers


def get_price_data(
    tickers: list, api_key: str, analysis_start_date: date, analysis_end_date: date
) -> list:

    current_date = analysis_start_date
    data = []
    while current_date <= analysis_end_date:
        current_date += timedelta(days=1)

        closing_prices = [
            parse_ticker_data(ticker=ticker, date=current_date, api_key=api_key)
            for ticker in tickers
        ]
        data = data + closing_prices

    return data


def calculate_monthly_cagr(df, analysis_start_date, analysis_end_date) -> dict:

    difference = relativedelta(analysis_start_date, analysis_end_date)
    total_months = difference.years * 12 + difference.months

    total_months = 1 if total_months == 0 else total_months

    monthly_cagr = df.withColumn(
        "monthly_cagr", (col("start_price") / col("end_price")) ** (1 / total_months)
    )
    max_monthly_cagr = monthly_cagr.agg({"monthly_cagr": "max"}).collect()[0][0]

    max_score_row = monthly_cagr.filter(monthly_cagr.monthly_cagr == max_monthly_cagr)

    return {
        "ticker": max_score_row.select(first("ticker")).collect()[0][0],
        "monthly_cagr": max_monthly_cagr,
    }


def create_report(
    start_date, end_date, relative_increase, monthly_cagr, investment_value
):
    with open(f"reports/report({datetime.now()}).txt", "w") as file:

        # Writing a header for the report
        file.write("Analysis Report\n")
        file.write("===============\n\n")

        file.write(f"Report generated at {datetime.now()}\n\n")

        # Writing the analysis results to the file
        file.write(f"Analysis period start: {start_date}\n")
        file.write(f"Analysis period end: {end_date}\n\n")

        file.write(
            f"{relative_increase['ticker']} had the greatest relative increase during the period.\n"
        )
        file.write(
            f"Relative increase in value: ${format(relative_increase['relative_increase']*100, '.2f')}%\n\n"
        )

        file.write(
            f"Buying $10,000 worth of shares of each compamy would have a total value of ${investment_value:,.2f} at the period end.\n\n"
        )

        file.write(
            f"{monthly_cagr['ticker']} had the highest CAGR gain over the period with {format(monthly_cagr['monthly_cagr'], '.3f')}%\n"
        )

    logging.debug("Execution successful")
