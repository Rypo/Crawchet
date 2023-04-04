import re
from collections import defaultdict

import html2text
import lxml.html
from bs4 import BeautifulSoup, Comment

import pandas as pd

from tqdm.auto import tqdm
from joblib import Parallel, delayed

#from crawchet.process.archive import ArchiveManager
from crawchet.utils import html as hutil, uri as uutil

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

class HTMLProcessor:
    def __init__(self, title_in_body=True, strip_images=True, strip_links=True, errors='ignore'):
        self.title_in_body = title_in_body
        self.strip_images = strip_images
        self.strip_links = strip_links
        self.errors = errors

    def preprocess(self, html_content, url=None, to_soup=False, fetch_remote_css=False):
        '''Attempt to make links absolute, inline css, and minify html before parsing.'''
        if len(html_content) == 0:
            return html_content

        html_content = hutil.try_make_absolutelinks(html_content, base_url=url, decode=True, errors=self.errors)    
        html_content = hutil.try_inline_css(html_content, fetch_remote_css=fetch_remote_css, remove_style_tags=True, errors=self.errors)
        html_content = hutil.try_minify_html(html_content, remove_bangs=False, errors=self.errors)

        html_content = html_content.encode()
        
        if to_soup:
            return BeautifulSoup(html_content, 'html.parser')
        
        return html_content


    def _extract_attrs(self, tag):
        if tag.name == 'a':
            return {k:v for k,v in tag.attrs.items() if k in ['href','title']}
        elif tag.name == 'img':
            return {k:v for k,v in tag.attrs.items() if k in ['href','src','width','height','alt','title']}
        
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

    def reduce_tagset(self, doc):
        # TODO: determine if just reinventing the wheel
        # https://github.com/buriy/python-readability
        # https://github.com/codelucas/newspaper
        # https://github.com/alan-turing-institute/ReadabiliPy
        # https://github.com/mozilla/readability
        if self.title_in_body and doc.title and doc.body:
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
            tag.attrs = self._extract_attrs(tag)

        # unnest div,p
        for t in doc(['div','p']):
            if t.parent.name==t.name and t.parent.text.strip()==t.text.strip():
                t.unwrap()

        for t in doc(['span']):
            if t.parent.name==t.name and t.parent.attrs.get('style','')==t.attrs.get('style',''):
                t.unwrap()

        # Unwrap links and images if text only
        if self.strip_images:
            for tag in doc('img'):
                tag.unwrap()

        if self.strip_links:
            for tag in doc('a'):
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

    def to_md(self, doc, base_url=""):
        
        if self.title_in_body and doc.title and doc.body:
            itag = doc.new_tag('strong')
            itag.string = doc.title.get_text(' ').strip()
            #doc.h1.insert_before(itag)
            
            doc.body.insert(0,itag)
            itag.wrap(doc.new_tag('h1'))

        h2t = html2text.HTML2Text(baseurl=base_url)
        #h2t.use_automatic_links=True
        #h2t.skip_internal_links=True
        #h2t.escape_snob=True
        h2t.ignore_images= self.strip_images
        h2t.ignore_links = self.strip_links
        #h2t.images_to_alt=True
        #h2t.default_image_alt='ALT'
        
        h2t.body_width=0
        #h2t.single_line_break=True
        return h2t.handle(doc.decode())

class GenericParser(HTMLProcessor):
    '''Generic HTML parser using BeautifulSoup for extracting text, links, and images from html content
    
    Args:
        tag_transform (str, {reduce, tokens, markdown, remove}): How tags are handled in text extraction (default: 'markdown')
            reduce: keep only text style tags (e.g. <b>, <i>, <u>, <s>)
            markdown: replace compatiable tags with markdown equivalent, remove others
            remove: remove all tags
        text_only (bool): If True, remove links (a) and images (img) (default: True)
        verbose (bool): Print debug info (default: False)
    '''
    def __init__(self, tag_transform='reduce', text_only=True, title_in_body=True, errors='ignore') -> None:
        self.tag_transform = tag_transform
        self.text_only = text_only

        super().__init__(title_in_body=title_in_body, strip_images=text_only, strip_links=text_only, errors=errors)

        
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
            doc = self.reduce_tagset(doc)
            return doc.decode(pretty_print=False)
        elif self.tag_transform == 'markdown':
            return self.to_md(doc, url)
        elif self.tag_transform == 'remove':
            return doc.get_text(' ')
        
        raise ValueError(f'Invalid tag_transform: {self.tag_transform}')


    def parse_page(self, html_content, url):
        if not html_content:
            return {'url':url, 'images':[], 'links':[], 'text':''}
        
        doc = self.preprocess(html_content, url, to_soup=True)
        
        doc_imgs = self.extract_images(doc)
        doc_links = self.extract_links(doc)
        doc_text = self.extract_text(doc, url)
        
        return {'url':url, 'images':doc_imgs, 'links':doc_links, 'text':doc_text}

    def parallel_parse(self, html_contents, urls, n_jobs=-5):
        return Parallel(n_jobs=n_jobs)(delayed(self.parse_page)(html,url) for html,url in tqdm(zip(html_contents, urls), total=len(html_contents)))



class FastParser:
    '''Generic minimal HTML parser using lxml for extracting text, links, and images from html content
    
    Args:
        errors (str, {ignore, replace, strict}): How to handle errors during preprocessing (default: 'ignore')
    '''
    def __init__(self, title_in_body=True, errors='ignore') -> None:
        self.title_in_body = title_in_body
        self.errors = errors

    def preprocess(self, html_content, url=None, to_lxml=False, fetch_remote_css=False):
        '''Attempt to make links absolute, inline css, and minify html before parsing.'''
        if len(html_content) == 0:
            return html_content

        html_content = hutil.try_make_absolutelinks(html_content, base_url=url, decode=True, errors=self.errors)    
        html_content = hutil.try_inline_css(html_content, fetch_remote_css=fetch_remote_css, remove_style_tags=True, errors=self.errors)
        html_content = hutil.try_minify_html(html_content, remove_bangs=False, errors=self.errors)

        html_content = html_content.encode()
        
        if to_lxml:
            return lxml.html.fromstring(html_content, base_url=url)
        
        return html_content
    
    def extract_images(self, ldoc):
        return [dict(i.attrib) for i in ldoc.xpath('//img')]

    def extract_links(self, ldoc):
        #if len(ldoc.xpath('//body')) == 0:
        #    return []
        links, link_descs = [], []
        for e,a,l,p in filter(lambda x: uutil.is_link_candidate(x,keep_img=False), ldoc.iterlinks()):
            if l not in links:
                links.append(l)
                link_descs.append(e.text_content().strip())
        
        return {'links':links, 'link_texts':link_descs}

    def _destroy_scripts(self, ldoc):
        for s in ldoc.xpath('//script'):
            s.getparent().remove(s)

    def extract_text(self, ldoc):
        # remove scripts before extracting text, lxml doesn't handle them well
        self._destroy_scripts(ldoc) 
        title_text = ''
        if self.title_in_body:
            title = ldoc.xpath('//title')
            if title:
                title = title[0]
                title_text = '<title>{}</title>'.format(title.text_content().strip()) 
                title.getparent().remove(title)

        text_iter = ldoc.body.itertext() if ldoc.xpath('//body') else ldoc.itertext()

        #rgx = re.compile(r'^(?:<!--|__wm\.).*', flags=re.DOTALL)
        text = title_text + ' '.join([t for t in map(str.strip, text_iter) if t and (not t.startswith('<!--'))])

        return text


    def parse_page(self, html_content, url):
        if not html_content:
            return {'url':url, 'images':[], 'links':[], 'link_texts': [], 'text':''}
        
        ldoc = self.preprocess(html_content, url, to_lxml=True)
        
        doc_imgs = self.extract_images(ldoc)
        doc_links_texts = self.extract_links(ldoc)
        doc_text = self.extract_text(ldoc)
        
        return {'url':url, 'images':doc_imgs, **doc_links_texts, 'text':doc_text}

    def parallel_parse(self, html_contents, urls, n_jobs=-5):
        return Parallel(n_jobs=n_jobs)(delayed(self.parse_page)(html,url) for html,url in tqdm(zip(html_contents, urls), total=len(html_contents)))



