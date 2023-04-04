import os
import io
import asyncio
import aiohttp
from aiohttp.resolver import AsyncResolver
from tqdm.auto import tqdm
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

from warcio.utils import BUFF_SIZE as WARCIO_BUFF_SIZE # 16384

from crawchet.utils import uri

class AsyncCrawler:
    def __init__(self, **session_kwargs) -> None:
        self.session_kwargs = session_kwargs
        self.pbar = None
        self._hits = {'OK':0, 'FAIL':0}

    def get_session(self):
        #timeout = aiohttp.ClientTimeout(connect=10)
        #UA = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
        UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        headers={'Accept-Encoding': 'identity', 'user-agent':UA}
        headers.update(self.session_kwargs.get('headers', {}))
        # Resolver kwargs
        ns = self.session_kwargs.get('nameservers', ["8.8.8.8", "8.8.4.4"])
        # Connector kwargs
        limit=self.session_kwargs.get('limit',100)
        limit_per_host=self.session_kwargs.get('limit_per_host',15)

        conn = aiohttp.TCPConnector(resolver=AsyncResolver(nameservers=ns), limit=limit, limit_per_host=limit_per_host)

        return aiohttp.ClientSession(trust_env = True, connector=conn, headers=headers)


    async def async_get(self, url, session: aiohttp.ClientSession):
        try:
            async with session.get(url=url, ssl=False) as resp:
                content = await resp.content.read()
            
            out_result=(url, content)
            self._hits['OK'] += 1
        except Exception as e:
            print(f"Failed to get: {url}\n Reason: {e}.")
            out_result=(url, None)
            self._hits['FAIL'] += 1
        finally:
            self.pbar.set_postfix(self._hits)
            self.pbar.update(1)
            
            return out_result

    async def crawl_urls(self, urls):
        self.pbar = tqdm(total=len(urls), postfix=self._hits)
        
        async with self.get_session() as session:
            ret = await asyncio.gather(*[self.async_get(url, session) for url in urls])
        
        print('Done. Results:', self._hits)
        
        return ret

class WARCAsyncCrawler(AsyncCrawler):
    def __init__(self, warc_outfile, **session_kwargs) -> None:
        self.warc_outfile = warc_outfile
        self.writer = None

        super().__init__(**session_kwargs)


    async def async_get(self, url, session: aiohttp.ClientSession):
        '''
        There is probably a better way to do this, right now it's a 'poor man's surrogate urllib3.response.HTTPResponse'.
        warcio expects a file-like object, but aiohttp returns a asyncio.StreamReader with resp.content.
        So, we read the stream into a io.BytesIO and wrap it in a io.BufferedReader to mock a file-like object.
        The issue arrises when warcio tries to create the digest using hashlib.update. (warcio/recordbuilder.py#L196)
        It rasises 'TypeError: object supporting the buffer API required'
        A possible approach may be mock out the digest, but the implications are unclear and may not even resolve the issue.
        '''
        try:
            async with session.get(url=url, ssl=False) as resp:
                #headers_list =[(k.decode(),v.decode()) for (k,v) in resp.raw_headers]
                headers_list = resp.raw_headers
                statusline=f'{resp.status} {resp.reason}'
                httpver=resp.version
                protocol=f'HTTP/{httpver.major}.{httpver.minor}'
                http_headers = StatusAndHeaders(statusline, headers_list, protocol=protocol)

                payload = await resp.content.read()
                payload = io.BufferedReader(io.BytesIO(payload), buffer_size=WARCIO_BUFF_SIZE)
                record = self.writer.create_warc_record(url, 'response', payload=payload, http_headers=http_headers)
                self.writer.write_record(record)
                self._hits['OK'] += 1
        except Exception as e:
            print(f"Failed to get: {url}\n Reason: {e}.")
            self._hits['FAIL'] += 1
        finally:
            self.pbar.set_postfix(self._hits)
            self.pbar.update(1)
                
    async def crawl_urls(self, urls):
        self.pbar = tqdm(total=len(urls), postfix=self._hits)

        try:
            output = open(self.warc_outfile, 'wb')
            self.writer = WARCWriter(output, gzip=True)
            
            async with self.get_session() as session:  
                await asyncio.gather(*[self.async_get(url, session) for url in urls])
        finally:
            self.writer.out.close()


class JsonAsyncCrawler(AsyncCrawler):
    def __init__(self, **session_kwargs) -> None:
        super().__init__(**session_kwargs)

    def _stack_header(self,header):
        sheader = {}
        for k in header.keys():
            v = header.getall(k)
            sheader[k] = v if len(v)>1 else v[0]

        return sheader

    async def async_get(self, url, session: aiohttp.ClientSession):
        keys = ('status','url','real_url','headers','content')
        try:
            async with session.get(url=url, ssl=False) as resp:
                status = resp.status
                
                headers = self._stack_header(resp.headers)
                real_url = resp.real_url.human_repr()
                print(f"{status} -- {url} ({real_url})")
                
                resp = await resp.text()
                
                print(f"Response Len: {len(resp)}\n")
            out_result=(status, url, real_url, headers, resp)
            self._hits['OK'] += 1
        except aiohttp.ClientResponseError as e:
            print("Unable to get url {} due to {}.".format(url, e.message))
            out_result=(e.status, url, e.request_info.real_url.human_repr(), e.headers, e.message)
            self._hits['FAIL'] += 1
        except Exception as e:
            print(f"Failed to get: {url}\n Reason: {e}.")
            out_result=(None, url, None, None, str(e))
            self._hits['FAIL'] += 1
        finally:
            self.pbar.set_postfix(self._hits)
            self.pbar.update(1)
            
            return dict(zip(keys,out_result))


class ImageAsyncCrawler(AsyncCrawler):
    def __init__(self, **session_kwargs) -> None:
        super().__init__(**session_kwargs)

    async def async_get(self, url, ptid, session: aiohttp.ClientSession):
        try:
            async with session.get(url=url, ssl=False) as resp:
                if resp.ok:
                    content = await resp.content.read()
                elif 'archive.org' in url:
                    url = ''.join(url.partition('/http')[1:])[1:]
                    async with session.get(url=url, ssl=False, raise_for_status=True) as iresp:
                        content = await iresp.content.read()
                else:
                    print(f"Unable to get url {url} due to ({resp.status}) {resp.reason}.")
                    content = None
                
            out_result=(url, ptid, content)
            self._hits['OK'] += 1
        except Exception as e:
            print(f"Failed to get: {url}\n Reason: {e}.")
            out_result=(url, ptid, None)
            self._hits['FAIL'] += 1

        finally:
            self.pbar.set_postfix(self._hits)
            self.pbar.update(1)
            
            return out_result


    async def crawl_urls(self, url_ptids):
        self.pbar = tqdm(total=len(url_ptids), postfix=self._hits)

        async with self.get_session() as session:
            ret = await asyncio.gather(*[self.async_get(url, ptid, session) for url,ptid in url_ptids])
        
        print('Done. Results:', self._hits)
        
        return ret