
class YahooFinanceParser():
    def __init__(self, tree, xpath_dict):
        self.tree = tree
        self.xpaths = xpath_dict
    
    def __repr__(self):
        return f'<YahooFinanceParser tree={"OK" if self.tree is not None else "None"}>'
    
    def _extract_single(self, path):
        try:
            value = self.tree.xpath(path)
            return value[0] if value else None
        except Exception as e:
            print(f'[ERROR] Extracting path {path}: {e}')
            return None
    
    def extract_metrics(self, ticker):
        extracted = {}
        for category, fields in self.xpaths.items():
            for name, xpath in fields.items():
                extracted[name] = self._extract_single(xpath)

        extracted['Ticker'] = ticker

        valid_fields = [v for k, v in extracted.items() if k != 'Ticker' and v is not None]
        if len(valid_fields) < 3:
            raise ValueError(f"No se extrajeron suficientes datos para {ticker}")
        return extracted