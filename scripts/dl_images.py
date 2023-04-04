import os
import json
import asyncio
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import lxml.html
from tqdm.auto import tqdm

from crawchet.utils import uri as uutil, io as ioutil
from crawchet.process import greatami
from crawchet.collect.crawl import ImageAsyncCrawler


def dl_greatami_imgs(ga_file, base_outdir, overwrite=False):
    df_gaf = greatami.process_gafile(ga_file=ga_file)
    df_gaimgs = greatami.extract_img_links(df_gaf)
    
    with requests.Session() as session:
        
        for img_url,ptid in tqdm(zip(df_gaimgs.imgurl, df_gaimgs.ptid), total=len(df_gaimgs)):    
            filepath = uutil.url_ptid_filepath(img_url, ptid, base_outdir, force_unique=False, as_str=False)    
            if not filepath.exists() or overwrite:
                try:
                    filepath.write_bytes(session.get(img_url).content)
                    print('wrote to:', filepath.as_posix())
                except Exception as e:
                    print(f'Failed: {img_url}\n Reason: {e}',img_url)


def write_image(url, ptid, content, base_outdir):
    try:
        filepath = uutil.url_ptid_filepath(url, ptid, base_outdir)
        with open(filepath, 'wb') as f:
            f.write(content)
    except Exception as e:
        print(e)

def write_images(out_results, base_outdir):
    for url,ptid,content in tqdm(out_results):
        if content is not None:
            write_image(url, ptid, content, base_outdir)


def extract_imgurls(readable_path):
    with open(readable_path) as f:
        df_readti = pd.DataFrame([r['data'] for r in json.load(f)])
    
    df_imgs = df_readti[['text','ptid']].copy()
    
    df_imgs['imgurl'] = df_imgs['text'].apply(lambda htx: lxml.html.fromstring(htx).xpath('//img/@src') if htx else [])
    df_imgs['imgurl'] = df_imgs['imgurl'].apply(lambda iurls: [*map(uutil.fix_imgurl,iurls)])
    
    df_imgs = df_imgs[['imgurl','ptid']].explode('imgurl').dropna().drop_duplicates('imgurl')
    
    return df_imgs


if __name__ == '__main__':
    img_dir =  ioutil.resolve_path('../data/raw/images/')
    readable_path = ioutil.resolve_path('../data/staged/jsonhtml/readabled.json')
    gafile_path = ioutil.resolve_path('../data/interim/greatamigurumi.json')

    #dl_greatami_imgs(gafile_path, img_dir, overwrite=False)

    df_gaimgs = greatami.extract_img_links(greatami.process_gafile(ga_file=gafile_path))
    iac = ImageAsyncCrawler()
    out_results = asyncio.get_event_loop().run_until_complete(iac.crawl_urls([*zip(df_gaimgs.imgurl, df_gaimgs.ptid)]))
    
    write_images(out_results, img_dir)
    
    df_imgs = extract_imgurls(readable_path)
    iac = ImageAsyncCrawler()
    out_results = asyncio.get_event_loop().run_until_complete(iac.crawl_urls([*zip(df_imgs.imgurl, df_imgs.ptid)]))
    
    write_images(out_results, img_dir)

    
    
    


    

