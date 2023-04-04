import json
from pathlib import Path

from crawchet.process import greatami
from crawchet.collect import scrape
from crawchet.utils.io import resolve_path

if __name__ == '__main__':
    warc_out = resolve_path('../data/raw/pages/greatamigurumi.warc.gz')
    json_out = resolve_path('../data/interim/greatamigurumi.json')
    img_dir =  resolve_path('../data/raw/images/greatamigurumi/')

    Path(warc_out).parent.mkdir(parents=True, exist_ok=True)
    Path(json_out).parent.mkdir(parents=True, exist_ok=True)
    Path(img_dir).mkdir(parents=True, exist_ok=True)
    
    ga_scraper = scrape.GreatAmigurumiScraper()
    gablog_data = ga_scraper.scrape(warc_out, timeout=1)
    
    with open(json_out,'w',encoding='UTF-8') as f:
        json.dump(gablog_data, f)
    