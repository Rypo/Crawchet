import re
from urllib import parse
from pathlib import Path
import lxml.html


def get_original_url(wb_url, strip_port=True):
    '''Extract original url from web.archive.org link
    
    https://web.archive.org/web/20221210031853/https://example.com/ -> https://example.com/
    '''
    if not isinstance(wb_url, str) or 'archive.org' not in wb_url:
        return wb_url
    
    url = ''.join(wb_url.partition('/http')[1:]).lstrip('/')
    if strip_port:
        url = re.sub(r':\d{1,5}/', '/', url)

    return url

def blogspot_full_size(src):
    '''
    blogspot image process params info
    - https://sneeit.com/using-blogger-blogspot-image-url-structure-for-cropping-and-resizing/
    - https://www.amp-blogger.com/2019/10/url-image-parameter-for-custom-blogger.html
    - https://gist.github.com/Sauerstoffdioxid/2a0206da9f44dde1fdfce290f38d2703
    '''
    return re.sub(r'/s\d{1,4}/', r'/s0/', src, flags=re.I)


def fix_urlscheme(url):
    ''' Fix common issues with url scheme'''
    if not isinstance(url,str):
        return url
    if url.startswith('//'):
        url = 'http:' + url
    elif url.startswith('://'):
        url = 'http' + url
    elif url.startswith('hhttp'):
        url = url.replace('hhttp','http', 1)
    
    return url


def fix_imgurl(src, resize_archive_link=False):
    '''Fix common issues with image urls'''
    src = fix_urlscheme(src)
    
    if not isinstance(src,str):
        return src
    # if it's an archived image, then it's unlikely that we can get the full size image
    if 'archive.org' not in src or resize_archive_link:
        src = blogspot_full_size(src)
    
    return src

def url_to_fname(url):
    '''
    https://1.bp.blogspot.com/.../.../.../.../s0/graduation_exam_success_crochet_bear.jpg ->
    graduation_exam_success_crochet_bear.jpg
    '''
    parse_res = parse.urlparse(url)
    fname = parse_res.path.split('/')[-1]
    fname = parse.unquote_plus(fname)
    fname = fname.replace(' ','_')
    return fname

def url_to_dirname(url):
    '''
    https://greatamigurumi.blogspot.com/2021/05/graduation-celebration-bear-free.html  ->
    2021-05-graduation-celebration-bear-free/
    '''
    dirname = parse.urlparse(url).path
    dirname = dirname.strip('/')
    dirname = dirname.replace('/','-')
    dirname = dirname.replace('.html','')
    return dirname


def url_ptid_filepath(url, ptid, base_dir, force_unique=False, as_str=True):
    out_path = Path(base_dir).joinpath(ptid)
    out_path.mkdir(parents=True, exist_ok=True)
    filename = url_to_fname(url)
    filepath = out_path.joinpath(filename)
    if filepath.exists() and force_unique:
        simcount = len(list(out_path.glob(f'*{filename}')))
        filepath = out_path.joinpath(f'{simcount}_{filename}')
    if as_str:
        filepath = filepath.as_posix()
    
    return filepath


def is_imageurl(link):
    linkpath = parse.urlparse(link).path
    match = re.search(r'\.(jpe?g|png|gif|bmp|webp|avif)', linkpath, re.I)
    return match is not None


def link_filter(link):
    '''Filter out images and links that will not contain HTML crochet patterns like youtube, amazon, ravelry...'''
    # TODO: use regex if faster/simpler
    url_ignore = ['www.amazon','amzn.to','amzn.com','ravelry.com','greatamigurumi.blogspot','mailto:','drive.google.com','youtube.com','facebook.com']
    ext_ignore = ['.pdf']#['.jpg','.png','.jpeg','.pdf','.gif','.bmp']
    llink = link.lower()
    return not (any(dom in llink for dom in url_ignore) or is_imageurl(llink) or any(llink.endswith(ext) for ext in ext_ignore))


def is_link_candidate(link_tup, keep_img=True):
    '''Return if is potentially useful link given tuple of (element, attribute, link, pos) from lxml iterlinks'''
    rgx = re.compile(r'^(?:javascript|mailto):.+|.*share.*', flags=re.I)

    element, attribute, link, pos = link_tup
    
    proper_link = element.tag in ['a']+(['img'] if keep_img else []) # ignore script, link, iframe 
    proper_link &= (rgx.search(link) is None) # ignore onclick js, emails, share links
    return proper_link

def classify_links(link_tups, host_name):
    '''Return dict of link and boolean properties for filtering'''
    groups = []
    for element, attribute, link, _ in link_tups:
        props = {
            'image': (element.tag.lower() == 'img'),
            'internal': (host_name in link),
            'schemeok': link.startswith('http'),
            'link': link,
        }
        groups.append(props)
    
    return groups


def parse_links(html_content:bytes, base_url, host_name=None):
    if host_name is None:
        host_name = parse.urlparse(base_url).netloc 

    ldoc = lxml.html.fromstring(html_content, base_url=base_url)
    ldoc.make_links_absolute()
    
    return classify_links(filter(is_link_candidate, ldoc.iterlinks()), host_name=host_name)