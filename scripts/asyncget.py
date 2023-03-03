import json
import pickle
from pathlib import Path

import asyncio

from crawchet.collect.crawl import JsonAsyncCrawler, WARCAsyncCrawler
from crawchet.process import greatami
from crawchet.utils.io import resolve_path


def dump_result(out_result, filename, pickle_onfail=True):
    try:
        json.dump(out_result,open(filename,'w'))
    except Exception as e:
        print("Unable to save JSON due to {}.".format(e))
        if pickle_onfail:
            try:    
                pickle.dump(out_result, open(filename.replace('.json','.pkl'),'wb'))
            except Exception as e:
                print("Unable to save pickle due to {}.".format(e))


def crawl_save_json(ga_file, outfile):
    cleaned_datelinks = greatami.read_datelinks(ga_file)
    cleaned_dates,cleaned_links = [*zip(*cleaned_datelinks)]
    
    jac = JsonAsyncCrawler()
    loop = asyncio.get_event_loop()
    out_result = loop.run_until_complete(jac.crawl_urls(cleaned_links))
    
    for i in range(len(out_result)):
        out_result[i]['post_date'] = str(cleaned_dates[i])
    
    dump_result(out_result, outfile, True)


def crawl_save_warc(urls_file, outfile):
    with open(urls_file, 'r') as f:
        nearest_urls = f.readlines()
    
    nearest_urls = list(map(str.strip,nearest_urls))
    
    wac = WARCAsyncCrawler(warc_outfile=outfile)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wac.warcwrite_async(nearest_urls))

if __name__ == '__main__':
    gafile_path = resolve_path('../data/raw/urls/greatamigurumi.json')
    json_out = resolve_path('../data/raw/pages/async_allresult.json')

    urllist_path = resolve_path('../data/raw/urls/url_list_patched.txt')
    warc_out = resolve_path('../data/raw/pages/patched_archives.warc.gz')
    

    crawl_save_json(gafile_path, json_out)
    crawl_save_warc(urllist_path, warc_out)
