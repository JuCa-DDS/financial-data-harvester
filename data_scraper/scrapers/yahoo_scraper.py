import requests
import time
import yaml
from requests.exceptions import RequestException
from urllib.parse import urljoin
from lxml import html

import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

from data_scraper.parsers.yahoo_parser import YahooFinanceParser
from utils.logger import LoggerService

BASE_URL = 'https://finance.yahoo.com/quote/' 
API_URL = 'https://api.scraperapi.com/'

class YahooFinanceScraper:
    def __init__(self, api, xpath_path):
        self.api_key = api
        self.logger = LoggerService(self.__class__.__name__).get_logger()

        self._payload = {
            'api_key':self.api_key,
            'premium':'true'
        }
        try:
            with open(xpath_path, 'r') as f:
                self.xpaths = yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f'[ERROR] XPath file not found at {xpath_path}')

    @property
    def payload(self):
        return self._payload.copy()
    
    def get_quote(self, ticker):
        url_ticker = urljoin(BASE_URL, f'{ticker}/')
        payload_ = self.payload
        payload_['url'] = url_ticker
        
        for attempt in range(3): 
            try:
                response = requests.get(self.api_url, params=payload_, timeout=30)
                if response.status_code == 200:
                    tree = html.fromstring(response.content)
                    parser = YahooFinanceParser(tree, self.xpaths)
                    return parser.extract_metrics('statistics', ticker)
                else:
                    self.logger.error(f'[ERROR] Status != 200 for {ticker}')
            except RequestException as e:
                self.logger.error(f'[ERROR] Attempt {attempt+1} failed:{e} for {ticker}')
            time.sleep(1 + attempt) 
    
    def get_statistics(self, ticker, normalize=False):
        url_ticker = urljoin(BASE_URL, f'{ticker}/')
        url_statistics = urljoin(url_ticker, 'key-statistics')
        payload_ = self.payload
        payload_['url'] = url_statistics

        for attempt in range(3):
            try:
                response = requests.get(API_URL, params=payload_, timeout=30)
                if response.status_code == 200:
                    tree = html.fromstring(response.content)
                    parser = YahooFinanceParser(tree, self.xpaths)
                    return parser.extract_metrics('statistics', ticker, normalize=normalize)
                else:
                    self.logger.error(f'[ERROR] Status != 200')
            except RequestException as e:
                self.logger.error(f'[ERROR] Attempt {attempt+1} failed:{e}')
            time.sleep(1 + attempt)
        return None
    
    def get_news_feed(self, ticker):
        url_news = f'http://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US'
        headers = {'User-Agent':'Mozilla/5.0'}
        
        try:
            response = requests.get(url_news, headers=headers, data={})
            root = ET.fromstring(response.content)

            items = []
            for item in root.findall('.//item'):
                title = item.find('title').text
                link = item.find('link').text
                pub_date = item.find('pubDate').text
                items.append({'title':title, 'link':link, 'published':pub_date})
            return items
        except Exception as e:
            return []
        
    def get_article_text(self, url):
        payload_ = self.payload
        payload_['url'] = url

        for attempt in range(3):
            try:
                response = requests.get(self.api_url, params=payload_, timeout=30)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    paragraphs = soup.find_all('p')
                    full_text = '\n'.join(p.get_text() for p in paragraphs)
                    return full_text
                else:
                    print('Respuesta Diferente')
            except RequestException as e:
                print(f'Intento {attempt+1} fallido: {e}')
            time.sleep(1 + attempt)
        return None
    
    def get_full_news(self, ticker):
        feed_news = self.get_news_feed(ticker)
        full_news = []

        for item in feed_news:
            link = item['link']
            article_text = self.get_article_text(link)
            full_news.append({
                'ticker':ticker,
                'link':link,
                'article_text':article_text
            })

        return full_news