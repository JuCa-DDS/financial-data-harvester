import os
import json
import yaml
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from tqdm import tqdm

from data_scraper.scrapers.yahoo_scraper import YahooFinanceScraper 
from data_scraper.concurrency.threadpool_scraper import YahooFinanceBatchScraper 

def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def append_jsonl(path, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def load_progress(progress_path):
    if progress_path.exists():
        with progress_path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return set(data.get('processed', [])), data.get('meta', {})
    return set(), {}

def save_progress(progress_path: Path, processed_set, meta=None):
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_path.open("w", encoding="utf-8") as f:
        json.dump({"processed": sorted(list(processed_set)),
                   "meta": (meta or {})}, f, ensure_ascii=False, indent=2)

load_dotenv()
API = os.getenv('SCRAPERAPI_KEY')
YAML_PATH = 'data_scraper/config/paths.yaml'

INPUT_CSV = 'Listado2.csv'
OUTPUT_DIR = Path('output/yahoo_stats')
OK_PATH = OUTPUT_DIR / 'ok.jsonl'
FAILED_PATH = OUTPUT_DIR / 'failed.jsonl'
PROGRESS = OUTPUT_DIR / 'progress.jsonl'

CHUNK_SIZE = 100

listado = pd.read_csv(INPUT_CSV)
tickers_all = listado['Tickers'].dropna().astype(str).unique().tolist()

done_set, meta = load_progress(PROGRESS)
tickers = [t for t in tickers_all if t not in done_set]

if not tickers:
    print('Todos los Tickers ya fueron procesados')
    raise SystemExit

scraper = YahooFinanceScraper(API, YAML_PATH)
batch = YahooFinanceBatchScraper(scraper, 15)

pbar = tqdm(total=len(tickers), desc='Downloading', unit='ticker')

for chunk in chunks(tickers_all, 12):
    try:
        result = batch.scrape_multiple(chunk, show_progress=False)

        ok_rows = []
        for r in result['ok']:
            row = {'ticker':r['ticker'], 'status':'ok'}
            row.update(r.get('data') or {})
            ok_rows.append(row)
        
        failed_rows = []
        for r in result['failed']:
            failed_rows.append({
                'ticker':r['ticker'],
                'status':'failed',
                'error':r.get('error', 'unknown')
            })

        append_jsonl(OK_PATH, ok_rows)
        append_jsonl(FAILED_PATH, failed_rows)

        for r in result['ok']:
            done_set.add(r['ticker'])
        for r in result['failed']:
            done_set.add(r['ticker'])
        
        save_progress(PROGRESS, done_set, meta={'chunksize':CHUNK_SIZE})
    
    except Exception as e:

        save_progress(PROGRESS, done_set, meta={'last_error':str(e)})
    
    finally:
        pbar.update(len(chunk))