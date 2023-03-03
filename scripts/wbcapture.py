'''This file must live in isolation from the rest of the project, because when warcio.capture_http is imported, it monkeypatches requests.
Any other imports of requests, either locally or via other packages prior to capture_http will cause capture_http to silently fail and write nothing to the the WARC file.
'''
import json
from urllib.parse import urlsplit
import pandas as pd
from tqdm.auto import tqdm

from warcio.capture_http import capture_http
import requests # requests must be imported after capture_http

from crawchet.process import greatami, archive
from crawchet.utils.io import resolve_path


def findall_wayback_urls(ga_file, wb_results_outfile, wburls_outfile):
    # TODO: make sure that capture_http isn't effected by the other imports required for this function
    df_dtlinks = pd.DataFrame(greatami.read_datelinks(ga_file), columns=['post_date','url'])
    df_dtlinks['post_date'] = archive.to_wbmts_format(df_dtlinks.post_date)

    df_dtlinks['url'] = df_dtlinks.url.str.strip('//').str.replace('hhttp','http')
    df_dtlinks['url'] = df_dtlinks.url.apply(urlsplit).apply(lambda u: f'{u.scheme}://{u.netloc}{u.path}')

    df_dtlinks = df_dtlinks.drop_duplicates()
    pre_archived = df_dtlinks[df_dtlinks.url.str.contains('web.archive.org')]
    df_dtlinks = df_dtlinks[~df_dtlinks.url.str.contains('web.archive.org')]

    wbrecords = archive.search_wayback(df_dtlinks.url, df_dtlinks.post_date)

    with open(wb_results_outfile,'w') as f:
        json.dump(wbrecords, f)

    print('Records written to:',wb_results_outfile)

    wbr_list = [wbr['archive_url'] for wbr in wbrecords if wbr['archive_url']]+pre_archived.url.to_list()

    with open(wburls_outfile,'w') as f:
        f.writelines([w+'\n' for w in wbr_list])

    print('Url list written to:', wburls_outfile)


def capture_wayback_requests(urls, warc_outfile):
    UA = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    headers={'Accept-Encoding': 'identity', 'user-agent':UA}
    with capture_http(warc_outfile), requests.Session() as session:
        for url in tqdm(urls):
            try:
                session.get(url, headers=headers)
            except Exception as e:
                print('Failed to capture:',url)
                print(e)


def read_wayback_urls(url_file):
    with open(url_file,'r') as f:
        wayback_urls = f.readlines()#.splitlines()

    wayback_urls = [*dict.fromkeys(map(str.strip,wayback_urls))]
    wayback_urls = [u for u in wayback_urls if 'web.archive.org' in u]

    return wayback_urls


if __name__ == '__main__':
    '''Be nice to the archive.org servers, keep the requests sequential.'''
    
    #wbpatches_file = Path('../data/raw/urls/only_wayback_patches.txt').resolve().as_posix()
    #wbreplace_warcout = Path('../data/raw/pages/only_wayback_replacements.warc.gz').resolve().as_posix()
    gafile_path = resolve_path('../data/raw/urls/greatamigurumi.json')
    all_wbresult_file = resolve_path('../data/raw/urls/waybacklinks_allresult.json')
    all_wburls_file = resolve_path('../data/raw/urls/archive_url_list.txt')

    all_wburls_warcout = resolve_path('../data/raw/pages/all_wayback_urls.warc.gz')


    #wayback_urls = read_wayback_urls(wbpatches_file)
    #capture_wayback_requests(wayback_urls, warc_outfile=wbreplace_warcout)

    findall_wayback_urls(gafile_path, all_wbresult_file, all_wburls_file)

    wayback_urls = read_wayback_urls(all_wburls_file)
    capture_wayback_requests(wayback_urls, warc_outfile=all_wburls_warcout)
