from concurrent.futures import ThreadPoolExecutor, as_completed
from data_scraper.scrapers.yahoo_scraper import YahooFinanceScraper
from tqdm import tqdm

class YahooFinanceBatchScraper:
    def __init__(self, scraper:YahooFinanceScraper, max_workers=15):
        self.scraper = scraper
        self.max_workers = max_workers

    def _resolve_fetcher(self, fetch, normalize=True, fetch_kwargs=None):
        fetch_kwargs = fetch_kwargs or {}

        if callable(fetch):
            return fetch
        
        if isinstance(fetch, str):
            f = fetch.lower()
            if f == 'statistics':
                return lambda t: self.scraper.get_statistics(t, normalize=normalize, **fetch_kwargs)
            if f == 'quote':
                return lambda t: self.scraper.get_quote(t, normalize=normalize, **fetch_kwargs)
        
        raise ValueError(f"Fetcher no valido: {fetch}")

    def _safe_scrape(self, ticker, fetcher):
        try:
            data = fetcher(ticker) 
            status = 'ok' if data else 'fail'
            return {'ticker':ticker, 'data':data, 'status':status}
        except Exception as e:
            return {'ticker':ticker, 'error':str(e), 'status':'fail'}
    
    def scrape_multiple(self, tickers, fetch='statistics', normalize=False, show_progress=True, tqdm_kwargs=None, fetch_kwargs=None):
        fetcher = self._resolve_fetcher(fetch, normalize=normalize, fetch_kwargs=fetch_kwargs)
        successful, failed = [], []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._safe_scrape, ticker, fetcher): ticker for ticker in tickers
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
