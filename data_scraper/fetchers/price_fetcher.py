import os
import datetime

import yfinance as yf

class YahooFinanceFetcher:
    def get_price(self, tickers, start_date, end_date):
        if isinstance(tickers, str):
            tickers = [tickers]

        data = yf.download(tickers, start=start_date, end=end_date)
        data_close = data['Close']
        data_close = data_close.reset_index()
        data_close.columns = ['Date', 'Close']
        return data_close
    
