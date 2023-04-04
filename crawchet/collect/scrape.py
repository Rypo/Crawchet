import time
from warcio.capture_http import capture_http
import requests # requests must be imported after capture_http
from bs4 import BeautifulSoup


class GreatAmigurumiScraper:
    def __init__(self) -> None:
        self.base_url = 'https://greatamigurumi.blogspot.com/'
        
    def link_image(self, tag):
        '''find <a href="..."> <img ... src="..."/></a>'''
        return tag.name=='a' and tag.find('img') #tag.next_element.name=='img'

    def extract_link_image(self, a_img):
        return {'img_link':a_img['href'], 'img_attrs':a_img.find('img').attrs}

    def build_text(self, a):
        '''Build up text from sibilings that surround <a> tag.
        This odd means of extracting text allows local text to be paired with its corresponding link and image.
        '''
        fullstr=''
        
        psib=a.previous_siblings
        nsib=a.next_siblings
        
        x=''
        while isinstance(x,str):
            fullstr+=x
            x=next(psib,None)

        fullstr+=a.get_text(' ')
        
        x=''
        while isinstance(x,str):
            fullstr+=x
            x=next(nsib,None)
            
        return fullstr.replace('\n','').replace('\xa0','')

    def extract_link_text(self, a):
        return {'text_link': a['href'], 'text_desc':self.build_text(a)}

    def body_parse(self, post_body):
        for x in post_body.find_all('div',class_='separator'):
            # These divs need to be removed in order for build_text to have proper text siblings 
            x.unwrap()
        
        post_body.smooth()
        parsed=[]
        for ai in post_body.find_all(self.link_image):
            item = self.extract_link_image(ai)
            # only match <a> that wraps text
            next_a = ai.find_next('a',text=True)
            item.update(self.extract_link_text(next_a))
            
            parsed.append(item)
            

        body_data = {
            'parsed':parsed,
            'raw_links': list(set([a.attrs.get('href','') for a in post_body.find_all('a')])),
            'raw_images':list(set([i.attrs.get('src','') for i in post_body.find_all('img')])),
            'raw_text':post_body.get_text(' ')#.text
        }
        
        return body_data


    def parse_post(self, post):
        post_date = post.find_previous(class_='date-header').text
        
        post_header = post.select_one('.post-title > a')
        header_data = {'title_text':'', 'title_link':''}
        if post_header:
            header_data = {'title_text':post_header.text, 'title_link':post_header['href']}
        
        post_body = post.select_one('.post-body')
        body_data = self.body_parse(post_body)
        
        post_footer = post.select_one('.post-footer')
        footer_data = [{'tag':cat.text, 'taglink':cat['href']} for cat in post_footer.select('.post-labels > a')]
        
        return {'post_date': post_date, 'header':header_data, 'body':body_data, 'footer':footer_data}


    def scrape(self, warc_outfile, timeout=2):
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        headers={'Accept-Encoding': 'identity', 'user-agent':ua}
        url = 'https://greatamigurumi.blogspot.com/search?updated-max=2023-12-31T03:09:00-07:00&max-results=64&start=0&by-date=false'
        site_data = []
        
        with capture_http(warc_outfile),requests.Session() as session:
            session.headers.update(headers)
            while url is not None:
                print(url)
                rsp = session.get(url)#, headers=headers)
                doc = BeautifulSoup(rsp.content, 'html.parser')
                posts = doc.find('div',class_='blog-posts').find_all('div',class_='post')
                page_data = []
                for i,post in enumerate(posts):
                    try:
                        page_data.append(self.parse_post(post))
                    except Exception as e:
                        print(f'failed entry: ({i})')
                        print(e)
                
                
                next_link = doc.find('a',text='Older Posts')
                next_url = next_link['href'] if next_link is not None else None
                    
                top_data = {'url': url, 'page_data':page_data, 'num_posts':len(posts), 'next_page':next_url}
                site_data.append(top_data)
                url = next_url
                time.sleep(timeout)
            
        return site_data
