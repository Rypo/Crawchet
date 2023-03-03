import io
import asyncio
import aiohttp
from aiohttp.resolver import AsyncResolver
from tqdm.auto import tqdm
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

from warcio.utils import BUFF_SIZE as WARCIO_BUFF_SIZE # 16384


class WARCAsyncCrawler:
    def __init__(self, warc_outfile) -> None:
        self.warc_outfile = warc_outfile
        self.writer = None

    def default_session(self):
        UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        headers={'Accept-Encoding': 'identity', 'user-agent':UA}
        resolver = aiohttp.resolver.AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
        conn = aiohttp.TCPConnector(resolver=resolver)

        return aiohttp.ClientSession(trust_env = True, connector=conn, headers=headers)


    async def async_get(self, url, session:aiohttp.ClientSession, pbar=None):
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
        except Exception as e:
            print('Failed to get:', url)
            print(e)
        finally:
            if pbar:
                pbar.update(1)
                
    async def warcwrite_async(self, urls):
        pbar = tqdm(total=len(urls))
        try:
            output = open(self.warc_outfile, 'wb')
            self.writer = WARCWriter(output, gzip=True)
            
            async with self.default_session() as session:  
                await asyncio.gather(*[self.async_get(url, session, pbar) for url in urls])
        finally:
            self.writer.out.close()


class JsonAsyncCrawler:
    def __init__(self) -> None:
        pass

    def _stack_header(self,header):
        sheader = {}
        for k in header.keys():
            v = header.getall(k)
            sheader[k] = v if len(v)>1 else v[0]

        return sheader


    def default_session(self):
        #timeout = aiohttp.ClientTimeout(connect=10)
        #UA = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
        UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        headers={'Accept-Encoding': 'identity', 'user-agent':UA}
        resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
        conn = aiohttp.TCPConnector(resolver=resolver)

        return aiohttp.ClientSession(trust_env = True, connector=conn, headers=headers)

    async def async_get(self, url, session: aiohttp.ClientSession, pbar=None):
        try:
            async with session.get(url=url, ssl=False) as resp:
                status = resp.status
                
                headers = self._stack_header(resp.headers)
                real_url = resp.real_url.human_repr()
                print(f"{status} -- {url} ({real_url})")
                
                resp = await resp.text()
                
                print(f"Response Len: {len(resp)}\n")
            out_result=(status, url, real_url, headers, resp)
        except aiohttp.ClientResponseError as e:
            print("Unable to get url {} due to {}.".format(url, e.message))
            out_result=(e.status, url, e.request_info.real_url.human_repr(), e.headers, e.message)
        except Exception as e:
            print("Unable to get url {} due to {}.".format(url, e))
            out_result=(None, url, None, None, str(e))

        finally:
            if pbar:
                pbar.update(1)
            keys = ('status','url','real_url','headers','content')
            return dict(zip(keys,out_result))


    async def crawl_urls(self, urls):
        pbar = tqdm(total=len(urls))
        async with self.default_session() as session:
            ret = await asyncio.gather(*[self.async_get(url, session, pbar) for url in urls])
        print("Finalized all. Return is a list of len {} outputs.".format(len(ret)))
        return ret