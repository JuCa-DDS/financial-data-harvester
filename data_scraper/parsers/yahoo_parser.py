
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
        except:
            return None
    
    def extract_metrics2(self):
        extracted = {}
        for category, fields in self.xpaths.items():
            for name, xpath in fields.items():
                extracted[name] = self._extract_single(xpath)
        
        return extracted