import requests
from requests.exceptions import RequestException
import os
import pandas as pd
import numpy as np
import yfinance as yf
import time
from dotenv import load_dotenv
from lxml import html

load_dotenv()
API = os.getenv('SCRAPERAPI_KEY')

class Download_YahooFinance:
    def __init__(self):
        self.URL_QUOTE = 'https://finance.yahoo.com/quote'
        self.URL_API = 'https://api.scraperapi.com/'
        self._PAYLOAD = {
            'api_key':API,
            'premium':'true'
        }
        self.COLUMNS_MULTIPLE = [
            'Company', 'Trailing P/E', 'Forward P/E', 'Price/Sales',
            'Ent. Value/EBITDA', 'EBITDA', 'Beta',
            '52 Week Change'
        ]

    @property
    def PAYLOAD(self):
        return self._PAYLOAD.copy()
    
    def __get_quote(self, ticker):
        url_ticker = os.path.join(self.URL_QUOTE, f'{ticker}')
        url_statistics = os.path.join(url_ticker, 'key-statistics')
        payload_ = self.PAYLOAD
        payload_['url'] = url_statistics

        for attempt in range(3):
            try:
                response = requests.get(self.URL_API, params=payload_, timeout=30)
                if response.status_code == 200:
                    return html.fromstring(response.content)
                else:
                    print('Dentro If')
            except RequestException as e:
                print(f'Intento {attempt+1} fallido: {e}')
            time.sleep(1 + attempt)
        return None
    
    def __extract_metrics(self, tree):
        # market_cap = tree.xpath('//*[@id="nimbus-app"]/section/section/section/article/section[2]/div/table/tbody/tr[1]/td[2]/text()')
        market_cap = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[1]/td[2]/text()')
        enterprise_value = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[2]/td[2]/text()')
        trailing_pe = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[3]/td[2]/text()')
        forward_pe = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[4]/td[2]/text()')
        peg_ratio_5yr = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[5]/td[2]/text()')
        price_sales = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[6]/td[2]/text()')
        price_book = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[7]/td[2]/text()')
        enterprise_value_revenue = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[8]/td[2]/text()')
        enterprise_value_ebitda = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/section[2]/div/table/tbody/tr[9]/td[2]/text()')

        profit_margin = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[2]/table/tbody/tr[1]/td[2]/text()')
        operating_margin = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[2]/table/tbody/tr[2]/td[2]/text()')

        revenue = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[4]/table/tbody/tr[1]/td[2]/text()')
        revenue_per_share = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[4]/table/tbody/tr[2]/td[2]/text()')
        quarterly_revenue_growth = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[4]/table/tbody/tr[3]/td[2]/text()')
        gross_profit = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[4]/table/tbody/tr[4]/td[2]/text()')
        ebitda = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[1]/div/section[4]/table/tbody/tr[5]/td[2]/text()')

        beta = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[2]/div/section[1]/table/tbody/tr[1]/td[2]/text()')
        week_change_52 = tree.xpath('//*[@id="nimbus-app"]/section/section/section/section/article/div/section[2]/div/section[1]/table/tbody/tr[2]/td[2]/text()')

        return {
            'Market_Cap': market_cap[0] if len(market_cap) == 1 else None,
            'Enterprise Value': enterprise_value[0] if len(enterprise_value) == 1 else None,
            'Trailing P/E': trailing_pe[0] if len(trailing_pe) == 1 else None,
            'Forward P/E': forward_pe[0] if len(forward_pe) == 1 else None,
            'PEG Ratio 5yr Expected': peg_ratio_5yr[0] if len(peg_ratio_5yr) == 1 else None,
            'Price/Sales': price_sales[0] if len(price_sales) == 1 else None,
            'Price/Book': price_book[0] if len(price_book) == 1 else None,
            'Ent. Value/Revenue': enterprise_value_revenue[0] if len(enterprise_value_revenue) == 1 else None,
            'Ent. Value/EBITDA': enterprise_value_ebitda[0] if len(enterprise_value_ebitda) == 1 else None,
            'Profit Margin': profit_margin[0] if len(profit_margin) == 1 else None,
            'Operating Margin': operating_margin[0] if len(operating_margin) == 1 else None,
            'Revenue': revenue[0] if len(revenue) == 1 else None,
            'Revenue Per Share': revenue_per_share[0] if len(revenue_per_share) == 1 else None,
            'Quarterly Revenue Growth': quarterly_revenue_growth[0] if len(quarterly_revenue_growth) == 1 else None,
            'Gross Profit': gross_profit[0] if len(gross_profit) == 1 else None,
            'EBITDA': ebitda[0] if len(ebitda) == 1 else None,
            'Beta': beta[0] if len(beta) == 1 else None,
            '52 Week Change': week_change_52[0] if len(week_change_52) == 1 else None
        }
    
    def get_info(self, ticker):
        tree = self.__get_quote(ticker)
        info = self.__extract_metrics(tree)
        return info
    
    def get_info_multiple(self, tickers):
        list_tickers = []
        for t in tickers:
            tree = self.__get_quote(t)
            info = self.__extract_metrics(tree)
            info['Company'] = t
            list_tickers.append(info)
        dataFrame = pd.DataFrame(list_tickers)
        return dataFrame[self.COLUMNS_MULTIPLE] 
    
    def get_price(self, ticker, start_date, end_date):
        close_price = yf.download(ticker, start_date, end_date)
        close_price = close_price.reset_index()
        close_price.columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume']
        return close_price