import os
import json
import pickle


import asyncio

from crawchet.collect.crawl import JsonAsyncCrawler, WARCAsyncCrawler
from crawchet.process import greatami
from crawchet.utils import io as ioutil


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
    df_dtlinks = greatami.get_datelinkdf(ga_file)
    cleaned_dates,cleaned_links = df_dtlinks.post_date.to_list(),df_dtlinks.url.to_list()
    
    jac = JsonAsyncCrawler()
    loop = asyncio.get_event_loop()
    out_result = loop.run_until_complete(jac.crawl_urls(cleaned_links))
    
    for i in range(len(out_result)):
        out_result[i]['post_date'] = str(cleaned_dates[i])
    
    dump_result(out_result, outfile, True)



def crawl_save_warc(url_list, outfile):
    if isinstance(url_list, str):
        url_list = ioutil.read_list(url_list)
    
    #url_list = list(map(str.strip,url_list))
    
    wac = WARCAsyncCrawler(warc_outfile=outfile)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wac.crawl_urls(url_list))

if __name__ == '__main__':
    urlfile_path = ioutil.resolve_path('../data/raw/urls/')
    gafile_path = ioutil.resolve_path('../data/interim/greatamigurumi.json')

    urllist_path = os.path.join(urlfile_path,'url_list.txt')
    wb_urllist_path = os.path.join(urlfile_path,'archive_url_list.txt')


    all_urls = ioutil.read_list(urllist_path, True) + ioutil.read_list(wb_urllist_path, True)
    
    crawl_save_warc(all_urls, ioutil.resolve_path('../data/raw/pages/merged.warc.gz'))
    