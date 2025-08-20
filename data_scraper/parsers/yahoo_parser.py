from data_scraper.parsers.normalizer import clean_numeric_with_suffix

class YahooFinanceParser():
    def __init__(self, tree, xpath_dict):
        self.tree = tree
        self.xpaths = xpath_dict
    
    def __repr__(self):
        return f'<YahooFinanceParser tree={"OK" if self.tree is not None else "None"}>'
    
    def _extract_first_match(self, paths):
        for path in paths:
            try:
                result = self.tree.xpath(path)
                if result or result is not None:
                    return result[0].strip() if isinstance(result[0], str) else result[0]
            except Exception as e:
                continue
        return None
    
    def extract_metrics(self, source, ticker, normalize=True):
        extracted = {}
        for category, fields in self.xpaths[source].items():
            for metric, paths in fields.items():
                raw_value = self._extract_first_match(paths)
                if normalize:
                    raw_value = clean_numeric_with_suffix(raw_value) 
                extracted[metric] = raw_value 

        # extracted['Ticker'] = ticker
        valid_fields = [v for k, v in extracted.items() if k != 'Ticker' and v is not None]
        # print(f'[DEBUG] Extracted {ticker} {extracted}')
        if len(valid_fields) < 1:
            raise ValueError(f"No se extrajeron suficientes datos para {ticker}")
        return extracted