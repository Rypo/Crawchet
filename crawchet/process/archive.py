import time
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter

from waybackpy import WaybackMachineCDXServerAPI
from waybackpy.exceptions import NoCDXRecordFound
import requests


def to_wbmts_format(post_dates: pd.Series):
    return post_dates.pipe(pd.to_datetime).dt.strftime('%Y%m%d')+'0'*6


def wayback_search(url, wayback_timestamp, timeout=0):
    UA = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    empty_record = dict.fromkeys(['status','available','url','timestamp'])
    try:
        rsp = requests.get('https://archive.org/wayback/available', params={'url':url, 'timestamp':wayback_timestamp}, timeout=32, headers={'User-Agent':UA})
        if rsp.status_code == 429:
            next_timeout = timeout+5
            print(f'Rate limited, waiting {next_timeout} seconds...')
            time.sleep(next_timeout)
            return wayback_search(url, wayback_timestamp, next_timeout)
        
        rsp.raise_for_status()    
        jrsp = rsp.json()
        record = {**jrsp['archived_snapshots'].get('closest',empty_record), 'original': jrsp['url']}
    except Exception as e:
        print(url,e)
        record = {**empty_record, 'original': url}
    
    record['archive_url'] = record.pop('url')
    return record

def wayback_search_all(urls, wayback_timestamps):
    return [wayback_search(url, wmts) for url,wmts in tqdm(zip(urls,wayback_timestamps), total=len(urls))]


def cdx_wayback_search(urls, wayback_timestamps):
    '''
    Search Wayback Machine for archive entries for each of the urls nearest to the timestamp.

    Args:
        urls (list): list of urls to search
        wayback_timestamps (list): list of timestamps for each url to search near (YYYYMMDDHHMMSS) 

    Returns:
        list: list of dicts containing archive metadata
    '''
    UA = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    archives = []
    empty_record = dict.fromkeys(['urlkey', 'timestamp', 'datetime_timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length', 'archive_url'])
    for url,wmts in tqdm(zip(urls,wayback_timestamps), total=len(urls)):
        try:
            res = WaybackMachineCDXServerAPI(url,user_agent=UA).near(wayback_machine_timestamp=wmts)
            record = res.__dict__
        except NoCDXRecordFound as e:
            print('No record found:',url)
            record = {**empty_record, 'original': url}

        except Exception as e:
            print('Error:',url)
            print(e)
            record = {**empty_record, 'original': url}
            
        finally:
            # remove redundant timestamp for serialization
            record.pop('datetime_timestamp')
            archives.append(record)

    return archives

class ArchiveManager:
    def __init__(self, warc_file=None) -> None:
        self.warc_file = warc_file
        

    def print_headers(self, warc_file=None):    
        warc_file = warc_file if warc_file is not None else self.warc_file
        
        with open(warc_file, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    print(record.http_headers)
                    print(record.rec_headers)
                    print('-'*100)


    def parse_records(self, warc_file=None):
        warc_file = warc_file if warc_file is not None else self.warc_file
            
        parsed = []
        with open(warc_file, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    extracts = {'statusline': record.http_headers.statusline}
                    rh_dict = dict(record.rec_headers.headers)
                    extracts.update({'target_uri':rh_dict['WARC-Target-URI'], 'content_length':rh_dict['Content-Length']})
                    extracts.update(content = record.content_stream().read())
                    parsed.append(extracts)

        return parsed

    def to_dataframe(self, warc_file=None):
        df_records = pd.DataFrame(self.parse_records(warc_file))
        df_records['content_length'] = df_records['content_length'].astype(int)
        df_records['status'] = df_records['statusline'].str.split().str[0].astype(int)
        df_records = df_records.drop(columns='statusline')
        
        return df_records

    def extract_metadata(self, archive_dir, warc_file=None):
        arcpath = Path(archive_dir)
        arcfiles = [arcpath/warc_file] if warc_file else arcpath.glob('*.warc.gz')
        
        record_extracts = []
        reckeys = ['WARC-Target-URI','WARC-Type','WARC-Payload-Digest','WARC-Block-Digest','Content-Length']
        for arc in arcfiles:
            print('Reading', arc.name)
            with arc.open('rb') as stream:
                for record in ArchiveIterator(stream):
                    record_extracts.append(
                        {'archive':arc.name,
                        'statusline':record.http_headers.statusline, 
                        **{k:record.rec_headers.get(k) for k in reckeys}}
                    )
        return record_extracts


    def merge_archives(self, archive_files, merged_outpath):
        #arcpath = 
        arcfiles = [Path(p) for p in archive_files]#[*arcpath.glob('*.warc.gz')]
        
        observed = set()
        with open(merged_outpath, 'wb') as output:
            writer = WARCWriter(output)
            for arc in arcfiles:
                print('Merging', arc.name)
                with arc.open('rb') as stream:
                    for record in ArchiveIterator(stream):
                        rec_head = record.rec_headers
                        warc_type,target_uri = rec_head.get('WARC-Type'), rec_head.get('WARC-Target-URI')
                         
                        if warc_type=='response' and target_uri not in observed:
                            writer.write_record(record)
                            observed.add(target_uri)
        
        print('Merged {} archives into {} records:  {})'.format(len(arcfiles), len(observed), merged_outpath))

