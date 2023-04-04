import os
import json
from urllib.parse import urlsplit

from crawchet.process import greatami, archive
from crawchet.utils import io as ioutil


def write_all_urls(ga_file, wb_results_outfile, wburls_outfile, urls_outfile):
    df_dtlinks = greatami.get_datelinkdf(ga_file)
    df_dtlinks['post_date'] = archive.to_wbmts_format(df_dtlinks.post_date)

    # clean malformed urls, strip query strings
    df_dtlinks['url'] = df_dtlinks.url.apply(urlsplit).apply(lambda u: f'{u.scheme}://{u.netloc}{u.path}')

    # remove duplicates and exclude pre-archived urls from url_list and wayback search
    df_dtlinks = df_dtlinks.drop_duplicates()
    pre_archived = df_dtlinks[df_dtlinks.url.str.contains('web.archive.org')]
    df_dtlinks = df_dtlinks[~df_dtlinks.url.str.contains('web.archive.org')]

    ioutil.write_list(df_dtlinks.url.to_list(), urls_outfile, drop_duplicates=True)
    print('Urls list written to:', urls_outfile)

    #wbrecords = archive.search_wayback(df_dtlinks.url, df_dtlinks.post_date)
    print('Searching wayback for archives... (takes a while, ~1.3 url/s)')
    wbrecords = archive.wayback_search_all(df_dtlinks.url, df_dtlinks.post_date)
    
    with open(wb_results_outfile,'w') as f:
        json.dump(wbrecords, f)

    print('Records written to:',wb_results_outfile)

    # write newly found archives links and pre-archived links to the list
    wbr_list = [wbr['archive_url'] for wbr in wbrecords if wbr['archive_url']] + pre_archived.url.to_list()

    ioutil.write_list(wbr_list, wburls_outfile, drop_duplicates=True)
    print('Archive urls list written to:', wburls_outfile)


# def read_wayback_urls(url_file):
#     with open(url_file,'r') as f:
#         wayback_urls = f.readlines()#.splitlines()

#     wayback_urls = [*dict.fromkeys(map(str.strip,wayback_urls))]
#     wayback_urls = [u for u in wayback_urls if 'web.archive.org' in u]

#     return wayback_urls


if __name__ == '__main__':
    
    url_dir = ioutil.resolve_path('../data/raw/urls')
    page_dir = ioutil.resolve_path('../data/raw/pages')

    #wbpatches_file = Path('../data/raw/urls/only_wayback_patches.txt').resolve().as_posix()
    #wbreplace_warcout = Path('../data/raw/pages/only_wayback_replacements.warc.gz').resolve().as_posix()
    gafile_path = ioutil.resolve_path('../data/interim/greatamigurumi.json') 
    
    wb_resfile_out = os.path.join(url_dir, 'waybacklinks_result.json')  
    wburl_list_out = os.path.join(url_dir, 'archive_url_list.txt') #
    url_list_out = os.path.join(url_dir, 'url_list.txt') 
    write_all_urls(gafile_path, wb_resfile_out, wburl_list_out, url_list_out)
    
    #wbpages_warc_out = os.path.join(page_dir, 'all_wayback_urls.warc.gz') # resolve_path('../data/raw/pages/all_wayback_urls.warc.gz')

    #wayback_urls = read_wayback_urls(wbpatches_file)
    #capture_wayback_requests(wayback_urls, warc_outfile=wbreplace_warcout)

    
    #wayback_urls = read_wayback_urls(wburl_list_out)
    #capture_wayback_requests(wayback_urls, warc_outfile=wbpages_warc_out)