import re
from collections import defaultdict
from tqdm.auto import tqdm
import pandas as pd
import lxml.html
import lxml.html.clean
from bs4 import BeautifulSoup, Comment

from joblib import Parallel, delayed
import html2text
import css_inline

import cchardet as chardet

#from crawchet.process.archive import ArchiveManager
from crawchet.process import archive
from crawchet.utils import html as hutils


RE_CROCHET_TERMS = re.compile(
    r'(?:\b|\d+)(sts?|ch|sl[ -]?st|inc|dec|sc|h?dc|rep|row|rnd|round)(?:\b|\d+)|single crochet|magic ring', 
    re.IGNORECASE|re.MULTILINE)

# https://www.w3schools.com/tags/ref_byfunc.asp
ALLOW_TAGS = frozenset([
    'a', # href
    'img', # src width height alt
    'h1','h2','h3','h4','h5','h6','title',
    'li','ol','ul',
    'pre','code',
    'table','tbody','thead','tfoot','td','th','tr',# colspan rowspan
    'u', # not md compatible
    's','del',
    'b','strong',
    'em','i',
    'br',
    'hr',
    'blockquote','q',
    'p','div' # 'span','section','article','aside','header','footer','nav','main','figure','figcaption',
])
TEXTLESS_TAGS = frozenset(['img','br','hr',])
DESTROY_TAGS = frozenset(['style', 'script', 'meta', 'link'])



def presoup(url, html_content):
    if not isinstance(html_content,bytes):
        html_content = html_content.encode()

    if html_content == b'':
        return html_content

    try:
        html_content = lxml.html.make_links_absolute(html_content, base_url=url)
        #html_content = lxml.html.clean.autolink_html(html_content) # this breaks encoding for some reason
    except Exception as e:
        #print(e)
        pass
    
    html_content = html_content.decode()#'ISO-8859-1')
    
    
    try:
        # Only UTF-8 for string representation. Other document encodings are not yet supported.
        html_content = css_inline.inline(html_content, remove_style_tags=True,  load_remote_stylesheets=False) # base_url=url,
    except Exception as e:
        #print(e)
        pass
        
    html_content = hutils.html_minify(html_content)
    html_content = html_content.encode()
    
    
    return html_content

def mkdoc(url, html_content, reduce=True, text_only=True):
    html_content = presoup(url, html_content)
    doc = BeautifulSoup(html_content, 'html.parser')
    if reduce:
        doc = reduce_tagset(doc, text_only=text_only)
    
    return doc


def _extract_attrs(tag):
    if tag.name == 'a':
        return {k:v for k,v in tag.attrs.items() if k in ['href','title']}
    elif tag.name == 'img':
        return {k:v for k,v in tag.attrs.items() if k in ['href','src','width','height','alt','title']}
    else:
        attrs = {}
        if tag.attrs:
            css_text_styles = frozenset([
                'font', 'font-style', 'font-weight', 'font-size',' font-varient', 
                'text-decoration', 'text-decoration-line', 'text-decoration-color', 'text-decoration-style', 'text-decoration-thickness', 
                'text-transform', 'color', 'background-color'])

            styles=tag.attrs.get('style','')
            text_styles = ';'.join([s.strip() for s in styles.split(';') if s.strip().split(':')[0] in css_text_styles])
            
            if text_styles:
                attrs = {'style':text_styles}
        
        return attrs

def reduce_tagset(doc, text_only=True):
    if doc.title and doc.body:
        itag = doc.new_tag('strong')
        itag.string = doc.title.get_text(' ').strip()
        
        doc.body.insert(0,itag)
        itag.wrap(doc.new_tag('h1'))
    
    # Remove whitespace from headers
    for h in doc.find_all(['h1','h2','h3','h4','h5','h6','title']):
        for estring in h.find_all(string=True):
            estring.replace_with(estring.strip())

    # Remove comments
    for com in doc(text=lambda text: isinstance(text, Comment)):
        com.extract()

    for tag in doc(True):
        # Destroy non-presentation tags
        tagname = tag.name
        if tagname in DESTROY_TAGS:
            tag.decompose()
        
        no_descendant_text = (tag.text.strip() == '')
        # Destroy tags with no text in self or descendants, unless textless
        if no_descendant_text and tagname not in TEXTLESS_TAGS:
            tag.decompose()
        elif not tag.string and tagname not in ALLOW_TAGS:
            tag.unwrap()

        # clean attributes of allowed tags
        tag.attrs = _extract_attrs(tag)

    # unnest div,p
    for t in doc(['div','p']):
        if t.parent.name==t.name and t.parent.text.strip()==t.text.strip():
            t.unwrap()

    for t in doc(['span']):
        if t.parent.name==t.name and t.parent.attrs.get('style','')==t.attrs.get('style',''):
            t.unwrap()


    # Unwrap links and images if text only
    if text_only:
        for tag in doc(['a','img']):
            tag.unwrap()
    else:
        # Unwrap a tags with non-http href          
        for atag in doc('a'):
            if not atag.attrs.get('href','').startswith('http'):
                atag.unwrap()

    # Convert br to newlines (to be processed by process_dftextmerged)
    #for br in doc('br'):
    #    br.replace_with('\n')

    doc.smooth()
    
    return doc



def to_md(doc, base_url="", strip_imgs_links=True):
    if doc.title and doc.body:
        itag = doc.new_tag('strong')
        itag.string = doc.title.get_text(' ').strip()
        #doc.h1.insert_before(itag)
        
        doc.body.insert(0,itag)
        itag.wrap(doc.new_tag('h1'))

    h2t = html2text.HTML2Text(baseurl=base_url)
    #h2t.use_automatic_links=True
    #h2t.skip_internal_links=True
    #h2t.escape_snob=True
    h2t.ignore_images= strip_imgs_links
    h2t.ignore_links = strip_imgs_links
    #h2t.images_to_alt=True
    #h2t.default_image_alt='ALT'
    
    h2t.body_width=0
    #h2t.single_line_break=True
    return h2t.handle(doc.decode())

class GenericParser:
    '''Generic HTML parser for extracting text, links, and images from html content
    
    Args:
        tag_transform (str, {reduce, tokens, markdown, remove}): How tags are handled in text extraction (default: 'markdown')
            reduce: keep only text style tags (e.g. <b>, <i>, <u>, <s>)
            markdown: replace compatiable tags with markdown equivalent, remove others
            remove: remove all tags
        text_only (bool): If True, remove links (a) and images (img) (default: True)
        verbose (bool): Print debug info (default: False)
    '''
    def __init__(self, tag_transform='reduce', text_only=True, verbose=False) -> None:
        self.tag_transform = tag_transform
        self.text_only = text_only
        self.verbose = verbose
        

    def extract_images(self, doc):
        return [i.attrs for i in doc.find_all('img')]

    def extract_links(self, doc):
        link_texts = defaultdict(list)
        for a in doc.select('a[href^="http"]'):
            url = a.attrs.get('href','')
            a.smooth()
            utext = a.get_text(' ').strip()
            if utext not in link_texts[url]:
                link_texts[url].append(utext)
                
        return [{'link':k, 'desc':'|'.join([desc for desc in v if desc])} for k,v in link_texts.items()]

            
    def extract_text(self, doc, url=""):
        if self.tag_transform == 'reduce':
            doc = reduce_tagset(doc, text_only=self.text_only)
            return doc.decode(pretty_print=False)
        elif self.tag_transform == 'markdown':
            return to_md(doc, url, strip_imgs_links=self.text_only)
        elif self.tag_transform == 'remove':
            return doc.get_text(' ')
        else:
            raise ValueError(f'Invalid tag_transform: {self.tag_transform}')


    def parse_page(self, url, html_content):
        if not html_content:
            return {'url':url, 'images':[], 'links':[], 'text':''}
        try:
            html_content = presoup(url, html_content)
        except Exception as e:
            if self.verbose: print(e)

        
        doc = BeautifulSoup(html_content, 'html.parser')
        
        doc_imgs = self.extract_images(doc)
        doc_links = self.extract_links(doc)
        doc_text = self.extract_text(doc, url)
        
        return {'url':url, 'images':doc_imgs, 'links':doc_links, 'text':doc_text}

    def parallel_parse(self, url_content, total=None, n_jobs=-5):
        return Parallel(n_jobs=n_jobs)(delayed(self.parse_page)(url,html) for url,html in tqdm(url_content, total=total))



def whitespace_transform(text_col, tag_transform='reduce'):
    text_col = (text_col
    .str.strip()
    .str.normalize('NFKC')
    .str.replace(r'(?:[ ]*\n[ ]*){2,}', r'\n', regex=True)) # newlines are entirely removed by label-studio regardless, so just any number of newlines with 1 newline
    
    return text_col


def process_dftextmerged(warc_file='../data/interim/merged.warc.gz', tag_transform='reduce', text_only=True, max_status=399, min_matches=0):
    print('processing warc file...')
    df_records = pd.DataFrame(archive.ArchiveManager(warc_file).parse_records())
    df_records['content_length'] = df_records['content_length'].astype(int)
    df_records['status'] = df_records['statusline'].str.split().str[0].astype(int)
    df_records = df_records.drop(columns='statusline')
    
    print('parsing html content...')
    gp = GenericParser(tag_transform, text_only, verbose=False)
    df_textracts = pd.DataFrame(gp.parallel_parse(df_records[['target_uri','content']].itertuples(index=False), total=len(df_records)))
    df_textracts = pd.concat([df_records[['status','content_length','content']], df_textracts],axis=1)
    
    if max_status>0:
        df_textracts = df_textracts[df_textracts.status.values<=max_status]
    
    print('cleaning and transforming whitespace text...')
    df_textracts['text'] = whitespace_transform(df_textracts['text'], tag_transform)

    print('counting crochet terms...')
    # Count crochet terms in text. Note: many non-english terms will be missed
    df_textracts['term_count'] = df_textracts['text'].str.count(RE_CROCHET_TERMS)
    
    if min_matches > 0:
        df_textracts = df_textracts[df_textracts.term_count>=min_matches]


    wb_msk = df_textracts.url.str.contains('web.archive.org/')
    # split out the original url from the web.archive.org url, remove the port number
    df_textracts.loc[wb_msk,'origurl'] = df_textracts[wb_msk].url.str.split(r'\d{14}/').str[1].str.replace(':80','')
    df_textracts['origurl'] = df_textracts['origurl'].fillna(df_textracts.url)

    print('extracting page titles...')
    if tag_transform == 'markdown':
        page_titles = df_textracts['text'].str.extract(r'^# [*]{2}(.+)[*]{2}')[0].fillna('')
    elif tag_transform == 'reduce':
        page_titles = df_textracts['text'].str.extract(r'<title>(.+)</title>')[0].fillna('')
    
    df_textracts.insert(df_textracts.columns.get_loc('text')+1, 'title', page_titles)
        
    return df_textracts


