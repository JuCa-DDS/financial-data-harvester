from concurrent.futures import ThreadPoolExecutor, as_completed
from data_scraper.scrapers.yahoo_scraper import YahooFinanceScraper
from tqdm import tqdm

class YahooFinanceBatchScraper:
    def __init__(self, scraper:YahooFinanceScraper, max_workers=15):
        self.scraper = scraper
        self.max_workers = max_workers

    def _safe_scrape(self, ticker):
        try:
            data = self.scraper.get_statistics(ticker)
            return {'ticker':ticker, 'data':data, 'status':'ok'}
        except Exception as e:
            return {'ticker':ticker, 'error':str(e), 'status':'fail'}
    
    def scrape_multiple(self, tickers, show_progress=True):
        successful = []
        failed = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._safe_scrape, ticker): ticker for ticker in tickers
            }

            iterator = as_completed(futures)

            if show_progress:
                iterator = tqdm(iterator, total=len(tickers), desc='Downloading')
            
            for future in iterator:
                ticker = futures[future]
                result = future.result()
                
                if result['status'] == 'ok':
                    successful.append(result)
                else:
                    failed.append(result)
                
        return {'ok':successful, 'failed':failed}
