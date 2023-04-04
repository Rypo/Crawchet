import re
import json
from typing import List, Dict, Union
from pathlib import Path
from datetime import datetime
import pandas as pd

from crawchet.utils import uri as uutil

def fix_bodylinks(body_extract):
    '''Modifies in-place'''
    for extr in body_extract:
        extr.update(img_link=uutil.fix_imgurl(extr.get('img_link')))
        extr.update(text_link=uutil.fix_urlscheme(extr.get('text_link')))
        extr['img_attrs'].update(src=uutil.fix_imgurl(extr['img_attrs'].get('src')))
        
    return body_extract

def extract_languages(df_gaf):
    # https://en.wikipedia.org/wiki/Languages_used_on_the_Internet
    langs = ['English','Russian','Spanish','French','German','Japanese','Turkish','Persian',
            'Portuguese','Italian','Chinese','Dutch','Vietnamese','Polish','Arabic','Korean',
            'Czech','Indonesian','Ukrainian','Greek','Hebrew','Thai','Swedish','Romanian',
            'Hungarian','Danish','Finnish','Slovak','Bulgarian','Serbian','Norwegian','Croatian',
            'Lithuanian','Slovenian','Norwegian','Catalan','Estonian','Latvian','Hindi']

    text_langs = df_gaf['raw_text'].str.extractall(f"({'|'.join(langs)})", re.I|re.M).groupby(level=0).agg(list)[0]
    tag_langs = df_gaf['tag_list'].apply(lambda x: list(set(x)&set(langs)))
    lang_extracts = text_langs.reindex(tag_langs.index, fill_value=[])+tag_langs
    lang_extracts = lang_extracts.apply(lambda x: list(dict.fromkeys([l.title() for l in x]))) # unique, preserve order
    return lang_extracts


def flatten_gadata(gadata: Union[str, Path, List[Dict]]) -> List[Dict]:
    if isinstance(gadata,(str, Path)):
        with open(gadata,'r') as f:
            gadata = json.load(f)
        
    flat_post_data = [post for page in gadata for post in page['page_data']]
    return flat_post_data

def process_gafile(ga_file='../data/interim/greatamigurumi.json'):    
    df_gaf = pd.json_normalize(flatten_gadata(ga_file))
    
    df_gaf.insert(0,'ptid',[f'{i:05d}' for i in range(len(df_gaf))])
    # extract just the tags, since links can be easily recovered
    df_gaf['tag_list'] = df_gaf.footer.apply(lambda taglist: [t.get('tag') for t in taglist])

    df_gaf = df_gaf.rename(columns={
        'header.title_text':'post_title',
        'header.title_link':'post_link',
        'body.parsed': 'body_extracts',
    }).rename(lambda c: c.replace('body.',''),axis=1)

    df_gaf['raw_text'] = (
        df_gaf['raw_text']
        .str.strip()
        .str.replace(r' {2,}',' ', regex=True)
        .str.replace(r' ?\n ?','\n', regex=True)
        .str.replace(r'\n',r'\\n', regex=True))
    
    df_gaf = df_gaf.drop(columns='footer')
    
    df_gaf['raw_images'] = df_gaf['raw_images'].apply(lambda img_urls: [uutil.fix_imgurl(img) for img in img_urls])
    df_gaf['raw_links'] = df_gaf['raw_links'].apply(lambda urls: [uutil.fix_urlscheme(url) for url in urls])
    df_gaf['body_extracts'].apply(fix_bodylinks) # In place OP

    df_gaf['languages'] = extract_languages(df_gaf)
    
    #df_gaf['dirslug'] = df_gaf['ptid'] + '_' + df_gaf['post_link'].apply(uutil.url_to_dirname)

    df_gaf['text_links'] = df_gaf['body_extracts'].apply(lambda subp: [i['text_link'] for i in subp])
    
    return df_gaf

def extract_img_links(df_gaf):
    '''
    Extracts all image links from raw_images, raw_links, and body_extracts columns and return frame with imgurl and ptid 
    '''
    df_gaimg = df_gaf[['ptid','raw_images','body_extracts','raw_links']].copy()
    df_gaimg['imgurl'] = (
        df_gaimg['raw_images']
        + df_gaimg['body_extracts'].apply(lambda xtrcs: [x['img_attrs']['src'] for x in xtrcs]
                                        + [*filter(uutil.is_imageurl, [x['img_link'] for x in xtrcs])])
        + df_gaimg['raw_links'].apply(lambda links: [*filter(uutil.is_imageurl,links)])
    )

    df_gaimg = df_gaimg[['imgurl','ptid']]
    df_gaimg = df_gaimg.explode('imgurl')
    df_gaimg['imgurl'] = df_gaimg['imgurl'].apply(uutil.fix_imgurl)
    df_gaimg = df_gaimg.drop_duplicates('imgurl')
    
    return df_gaimg

def get_datelinkdf(df_gaf):
    if isinstance(df_gaf, (str, Path)):
        df_gaf = process_gafile(df_gaf)
    df_dlinks = df_gaf[['post_date','raw_links']].copy()
    df_dlinks['post_date'] = pd.to_datetime(df_dlinks.post_date, format='%b %d, %Y').dt.date
    df_dlinks = df_dlinks.explode('raw_links')
    df_dlinks = df_dlinks[df_dlinks.raw_links.apply(uutil.link_filter)]
    df_dlinks = df_dlinks.rename(columns={'raw_links':'url'})

    return df_dlinks