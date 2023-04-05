import re
import json
import yaml
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from joblib import Parallel,delayed
from readabilipy import simple_json_from_html_string

from crawchet.process import archive, parsehtml, greatami
from crawchet.utils import uri as uutil


# https://github.com/megagonlabs/tagruler
# https://github.com/jiesutd/YEDDA
# https://github.com/argilla-io/argilla#key-features
# https://explosion.ai/blog/spancat

RE_CROCHET_TERMS = re.compile( # Note: many non-english terms are not accounted for
    r'(?:\b|\d+)(sts?|ch|sl[ -]?st|inc|dec|sc|h?dc|rep|row|rnd|round)(?:\b|\d+)|single crochet|magic ring', 
    re.IGNORECASE|re.MULTILINE)


def warc_to_dftext(warc_file='../data/interim/merged.warc.gz', tag_transform='reduce', text_only=True, max_status=399, min_term_count=0):
    print('processing warc file...')
    df_records = archive.ArchiveManager().to_dataframe(warc_file)
    
    print('parsing html content...')
    if tag_transform == 'fast':
        gp = parsehtml.FastParser(title_in_body=True)
    else:
        gp = parsehtml.GenericParser(tag_transform, text_only, title_in_body=True)
    
    df_textracts = pd.DataFrame(gp.parallel_parse(df_records['content'], df_records['target_uri']))
    df_textracts = pd.concat([df_records[['status','content_length','content']], df_textracts], axis=1)
    
    if max_status>0:
        df_textracts = df_textracts[df_textracts.status.values<=max_status]
    
    print('cleaning and transforming whitespace text...')
    df_textracts['text'] = (
        df_textracts['text']
        .str.strip()
        .str.normalize('NFKC')
        .str.replace(r'(?:[ ]*\n[ ]*){2,}', r'\n\n', regex=True)
    ) # newlines are entirely removed by label-studio regardless, so just any number of newlines with 2 newlines

    print('counting crochet terms...')
    df_textracts['term_count'] = df_textracts['text'].str.count(RE_CROCHET_TERMS)
    df_textracts = df_textracts[df_textracts.term_count>=min_term_count] # filter out pages with too few crochet terms

    df_textracts['origurl'] = df_textracts.url.apply(uutil.get_original_url, strip_port=True)
    #wb_msk = df_textracts.url.str.contains('web.archive.org/')
    # split out the original url from the web.archive.org url, remove the port number
    #df_textracts.loc[wb_msk,'origurl'] = df_textracts[wb_msk].url.str.split(r'\d{14}/').str[1].str.replace(':80','')
    #df_textracts['origurl'] = df_textracts['origurl'].fillna(df_textracts.url)

    print('extracting page titles...')
    if tag_transform == 'markdown':
        page_titles = df_textracts['text'].str.extract(r'^# [*]{2}(.+)[*]{2}')[0].fillna('')
    elif tag_transform in ['reduce','fast']:
        page_titles = df_textracts['text'].str.extract(r'<title>(.+)</title>')[0].fillna('')
    
    df_textracts.insert(df_textracts.columns.get_loc('text')+1, 'title', page_titles)
        
    return df_textracts


def build_masterframe(ga_file, warc_file, df_master_outfile=None, tag_transform='reduce'):
    ''' Build the final combined dataframe from the raw data files.
    
    ga_file = '../data/interim/greatamigurumi.json'
    warc_file= '../data/interim/merged.warc.gz'
    df_master_outfile = '../data/interim/df_master.pkl'
    '''
    print('Processing Great Amigurumi Files')
    df_gaf = greatami.process_gafile(ga_file)

    print('Transforming WARC Files into Text Extracts Dataframe')
    df_textracts = warc_to_dftext(warc_file= warc_file, tag_transform=tag_transform, text_only=True, max_status=399, min_term_count=0)

    print('Merging Dataframes')
    df_master = df_textracts.merge(df_gaf.explode('text_links'), left_on='origurl', right_on='text_links')
    
    # drop where text is identical
    df_dedup = df_master.drop_duplicates('text')
    # drop where url is repeated, accounting for web.archive.org urls
    # prefering the highest crocheted term count followed by longest content length
    df_dedup = df_dedup.sort_values(['origurl','term_count','content_length'], ascending=(True,False,False)).drop_duplicates('origurl',keep='first')

    if df_master_outfile is not None:
        df_dedup.to_pickle(df_master_outfile)

    return df_dedup


def write_json(df_dedup, outfile):
    ''' Write the deduplicated dataframe to a json file for import into label-studio.'''
    df_jsub = df_dedup[['ptid','text','post_title','languages','term_count','content_length','url']].copy()
    dedupe_json = [{'data': rec } for rec in df_jsub.to_dict('records')]
    
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    
    with open(outfile,'w') as f:
        json.dump(dedupe_json,f)

    return df_dedup

def parallel_simplify(df_master_path='../data/interim/df_master.pkl', json_out_path='../data/staged/simphtml.json', n_jobs=-5):
    df_master = pd.read_pickle(df_master_path)
    hproc = parsehtml.HTMLProcessor()
    
    pre_parsed = Parallel(n_jobs)(delayed(hproc.preprocess)(c,u,to_soup=False) for c,u in tqdm(zip(df_master.content,df_master.url), total=len(df_master)))

    #readable_jsons = Parallel(n_jobs)(delayed(simplify_html)(c.decode()) for c in tqdm(pre_parsed))
    simphtml_jsons = Parallel(n_jobs)(delayed(simple_json_from_html_string)(c.decode(), use_readability=True) for c in tqdm(pre_parsed))
    
    df_rdable = pd.DataFrame(simphtml_jsons, index=df_master.index)
    df_rdable['ttext'] = (('<h1><strong>'+df_rdable.title+'</strong></h1>')+df_rdable.content).fillna('')
    df_master['text'] = df_rdable['ttext']
    write_json(df_master, json_out_path)
    
    return df_master



def text_filenames(df_ddjoined, ext='md'):
    filenames = (
        df_ddjoined.term_count.astype(str)+'_'+df_ddjoined.content_length.astype(str)+'_'
        + df_ddjoined.post_title
            .str.replace(r'[^a-zA-Z0-9 ]','',regex=True)
            .str.split()
            .str[:4]
            .str.join('_')
        +f'.{ext}')
    return filenames


def write_html(df_ddjoined, outdir='../data/staged/html/'):
    filenames = text_filenames(df_ddjoined, ext='html')    
    stpath = Path(outdir)
    
    for fname, text in tqdm(zip(filenames, df_ddjoined.text), total=len(filenames)):
        stpath.joinpath(fname).write_text(text)
    
    return df_ddjoined


def write_markdown(df_ddjoined, outdir='../data/staged/'):
    # how to pandoc all markdown files to html for import into label studio  
    # for f in `ls ../*/*/*.md`; do pandoc -f markdown -t html "$f" -o ./$(basename "$f" .md).html; done
    def _build_front_matter(row):
        row_dict = {k:(v.item() if isinstance(v,np.integer) else v) for k,v in row.to_dict().items()}
        return '---\n' + yaml.safe_dump(row_dict, default_flow_style=None, sort_keys=False, width=float("inf")) + '---\n\n'

    filenames = text_filenames(df_ddjoined, 'md')

    df_frontmatter = df_ddjoined[['title','post_title','raw_text','languages','tag_list','term_count','content_length','url']]
    front_matter = df_frontmatter.apply(_build_front_matter, axis=1)
    
    stpath = Path(outdir)
    
    for slug, fname, text, fmatter in tqdm(zip(df_ddjoined.dirslug, filenames, df_ddjoined.text, front_matter),total=len(filenames)):
        tpath = stpath.joinpath(slug,'texts')
        tpath.mkdir(parents=True, exist_ok=True)
        tpath.joinpath(fname).write_text(fmatter+text)
    
    return df_ddjoined