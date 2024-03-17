"""
Tests for get_price_data()
"""

from src.main import get_price_data, valid_date
from datetime import date


def test_get_price_data():
    """
    Writing successful a test for get_price_data()
    """
    tickers = ["AAPL", "MSFT"]
    get_price_data(tickers, date(2022, 6, 2), date(2022, 6, 3))
