import re
import json
import urllib
from typing import List, Dict, Union
from pathlib import Path
from datetime import datetime
import pandas as pd

def imgurl_fix(src, large=True):
    '''
    blogspot image process params info
    - https://sneeit.com/using-blogger-blogspot-image-url-structure-for-cropping-and-resizing/
    - https://www.amp-blogger.com/2019/10/url-image-parameter-for-custom-blogger.html
    - https://gist.github.com/Sauerstoffdioxid/2a0206da9f44dde1fdfce290f38d2703
    '''
    if not isinstance(src,str):
        return src
    if src.lower().startswith('//'):
        src = 'http:'+src
    if large:
        src = re.sub(r'/s\d{1,3}/', r'/s1600/', src, flags=re.IGNORECASE)
    return src

def imgurls_fix(img_urls, large=True):
    return [imgurl_fix(img, large) for img in img_urls]

def fix_bodylinks(body_extract):
    '''Modifies in-place'''
    for extr in body_extract:
        extr.update(img_link=imgurl_fix(extr.get('img_link'), large=False))
        extr.update(text_link=imgurl_fix(extr.get('text_link'), large=False))
        extr['img_attrs'].update(src=imgurl_fix(extr['img_attrs'].get('src'), large=True))
        
    return body_extract

def url_to_fname(url):
    '''
    https://1.bp.blogspot.com/.../.../.../.../s1600/graduation_exam_success_crochet_bear.jpg ->
    graduation_exam_success_crochet_bear.jpg
    '''
    parse_res = urllib.parse.urlparse(url)
    fname = parse_res.path.split('/')[-1]
    fname = urllib.parse.unquote_plus(fname)
    fname = fname.replace(' ','_')
    return fname

def url_to_dirname(url):
    '''
    https://greatamigurumi.blogspot.com/2021/05/graduation-celebration-bear-free.html  ->
    2021-05-graduation-celebration-bear-free/
    '''
    dirname = urllib.parse.urlparse(url).path
    dirname = dirname.strip('/')
    dirname = dirname.replace('/','-')
    dirname = dirname.replace('.html','')
    return dirname

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
    lang_extracts = lang_extracts.apply(lambda x: list(dict.fromkeys([l.title() for l in x]).keys())) # unique, preserve order
    return lang_extracts


def flatten_gadata(gadata: Union[str, Path, List[Dict]]) -> List[Dict]:
    if isinstance(gadata,(str, Path)):
        with open(gadata,'r') as f:
            gadata = json.load(f)
        
    flat_post_data = [post for page in gadata for post in page['page_data']]
    return flat_post_data

def process_gafile(ga_file='../data/raw/urls/greatamigurumi.json'):    
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
    
    df_gaf['raw_images'] = df_gaf['raw_images'].apply(imgurls_fix)
    df_gaf['raw_links'] = df_gaf['raw_links'].apply(imgurls_fix, large=False)
    df_gaf['body_extracts'].apply(fix_bodylinks) # In place OP

    df_gaf['languages'] = extract_languages(df_gaf)
    
    df_gaf['dirslug'] = df_gaf['ptid'] + '_' + df_gaf['post_link'].apply(url_to_dirname)

    df_gaf['text_links'] = df_gaf['body_extracts'].apply(lambda subp: [i['text_link'] for i in subp])
    
    return df_gaf



def is_imageurl(link):
    linkpath = urllib.parse.urlparse(link).path
    if re.search(r'\.(jpe?g|png|gif|bmp|webp|avif)', linkpath, re.IGNORECASE):
        return True
    return False


def extract_links(df_gaf):
    '''
    Extracts all the links from the body_extracts and raw_links columns
    '''
    raw_images = df_gaf['raw_images']
    bodyex_images = df_gaf['body_extracts'].apply(lambda exts: [e['img_attrs']['src'] for e in exts])
    bodyex_ilinks = df_gaf['body_extracts'].apply(lambda xtrcs: [*filter(is_imageurl,[x['img_link'] for x in xtrcs])])
    
    return raw_images,bodyex_images,bodyex_ilinks



def link_clean(link):
    # TODO: use regex if faster/simpler
    url_ignore = ['www.amazon','amzn.to','amzn.com','ravelry.com','greatamigurumi.blogspot','mailto:','drive.google.com','youtube.com','facebook.com']
    ext_ignore = ['.jpg','.png','.jpeg','.pdf','.gif','.bmp']
    llink = link.lower()
    return not (any(dom in llink for dom in url_ignore) or any(llink.endswith(ext) for ext in ext_ignore))


def read_datelinks(ga_file):
    with open(ga_file,'r') as f:
        flat_post_data = flatten_gadata(json.load(f))
    
    date_links = [(fpd['post_date'],fpd['body']['raw_links']) for fpd in flat_post_data]
    # month day, Year (%b %d, %Y) -> datetime.date()
    cleaned_date_links = [(datetime.strptime(pdate, '%b %d, %Y').date(),link) for (pdate,link_set) in date_links for link in filter(link_clean,link_set)]
    return cleaned_date_links