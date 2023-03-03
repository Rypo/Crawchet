import json
import yaml
from pathlib import Path
import numpy as np
from tqdm.auto import tqdm
from crawchet.process import parsehtml, greatami

def build_masterframe(ga_file, warc_file, df_master_outfile, tag_transform='reduce', outdir=None):
    ''' Build the final combined dataframe from the raw data files.
    
    ga_file = '../data/raw/urls/greatamigurumi.json'
    warc_file= '../data/interim/merged.warc.gz'
    df_master_outfile = '../data/interim/df_master.pkl'

    '''
    print('Processing Great Amigurumi Files')
    df_gaf = greatami.process_gafile(ga_file)

    print('Transforming WARC Files into Text Extracts Dataframe')
    df_textracts = parsehtml.process_dftextmerged(warc_file= warc_file, tag_transform=tag_transform, text_only=True, max_status=399, min_matches=0)


    print('Merging Dataframes')
    df_master = df_textracts.merge(df_gaf.explode('text_links'), left_on='origurl', right_on='text_links')
    
    
    # drop where text is identical
    df_dedup = df_master.drop_duplicates('text')
    # drop where url is repeated, accounting for web.archive.org urls
    # prefering the highest crocheted term count followed by longest content length
    df_dedup = df_dedup.sort_values(['origurl','term_count','content_length'],ascending=(True,False,False)).drop_duplicates('origurl',keep='first')

    df_dedup.to_pickle(df_master_outfile)

    if outdir is not None:
        if tag_transform == 'markdown':
            df_dedup = write_markdown(df_dedup, outdir=outdir)
        elif tag_transform == 'reduce':
            df_dedup = write_html(df_dedup, outdir=outdir)

    return df_dedup


def write_json(df_dedup, outfile='../data/staged/jsonhtml/deduped.json'):
    ''' Write the deduplicated dataframe to a json file for import into label studio.'''
    df_jsub = df_dedup[['ptid','text','post_title','languages','term_count','content_length','url']].copy()
    dedupe_json = [{'data': rec } for rec in df_jsub.to_dict('records')]
    
    with open(outfile,'w') as f:
        json.dump(dedupe_json,f)

    return df_dedup


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


def build_front_matter(row):
    row_dict = {k:(v.item() if isinstance(v,np.integer) else v) for k,v in row.to_dict().items()}
    return '---\n' + yaml.safe_dump(row_dict, default_flow_style=None, sort_keys=False, width=float("inf")) + '---\n\n'

def write_markdown(df_ddjoined, outdir='../data/staged/'):
    filenames = text_filenames(df_ddjoined, 'md')

    df_frontmatter = df_ddjoined[['title','post_title','raw_text','languages','tag_list','term_count','content_length','url']]
    front_matter = df_frontmatter.apply(build_front_matter, axis=1)
    
    stpath = Path(outdir)
    
    for slug, fname, text, fmatter in tqdm(zip(df_ddjoined.dirslug, filenames, df_ddjoined.text, front_matter),total=len(filenames)):
        tpath = stpath.joinpath(slug,'texts')
        tpath.mkdir(parents=True, exist_ok=True)
        tpath.joinpath(fname).write_text(fmatter+text)
    
    return df_ddjoined


# https://github.com/megagonlabs/tagruler
# https://github.com/jiesutd/YEDDA
# https://github.com/argilla-io/argilla#key-features
# https://explosion.ai/blog/spancat


# how to pandoc all markdown files to html for import into label studio  
# for f in `ls ../*/*/*.md`; do pandoc -f markdown -t html "$f" -o ./$(basename "$f" .md).html; done