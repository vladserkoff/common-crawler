# pylint: disable = redefined-builtin
"""
Interface to Common Crawl
"""
import datetime
import email
import gzip
import io
import json
import random
import time
from typing import Dict, List, Optional
from urllib.parse import unquote, urljoin

import boto3
import requests
from botocore.handlers import disable_signing
from cytoolz.curried import concat, filter, map, pipe, unique


class CommonCrawler:
    """
    Interaface to Common Crawl. See http://commoncrawl.org/
    """

    def __init__(self,
                 index_host: str = 'http://index.commoncrawl.org',
                 max_retries: int = 5,
                 recent_indexes: int = 1) -> None:
        """
        Query common crawl for htmls.

        Parameters
        ----------
        max_retries : int, default 5
            Number of http connection retries.
        recent_indexes : int, default 1
            Number of crawls to search in.
        """
        self.indexes_list = urljoin(index_host, 'collinfo.json')
        self.base_url = 'https://commoncrawl.s3.amazonaws.com'

        self.session = self._get_requests_session(max_retries)
        self.indexes = self.load_indexes(recent_indexes)
        self.bucket = self._get_crawl_bucket()

    def _get_requests_session(self, max_retries: int) -> requests.Session:
        session = requests.session()
        adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
        session.mount(self.base_url, adapter)
        return session

    def load_indexes(self, recent_k: int = 5) -> List[str]:
        """
        Get list of available Common Crawl indexes.

        Notes
        -----
        Sometimes the list of vailable indexes is not reachable,
        try to get it at most 10 times, then throw an exception.
        """
        for _ in range(10):
            resp = self.session.get(self.indexes_list)
            if resp.reason == 'OK':
                break
            else:
                time.sleep(random.randint(1, 3))
        else:
            raise IOError('Common crawl index is unreachable.')

        indexes = resp.json()
        indexes = [ind['cdx-api'] for ind in indexes]
        if recent_k:
            indexes = indexes[0:recent_k]
        return indexes

    @staticmethod
    def _get_crawl_bucket():
        aws_s3 = boto3.resource('s3')
        aws_s3.meta.client.meta.events.register('choose-signer.s3.*',
                                                disable_signing)
        bucket = aws_s3.Bucket('commoncrawl')
        return bucket

    @staticmethod
    def _cur_ts() -> str:
        """
        Current 'timestamp' in Common Crawl index format.
        """
        return datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')

    def find_domain_urls(self, domain: str) -> List[str]:
        """
        Get all known urls for domain.

        Returns
        -------
        all_urls : iterator
        """

        def _urlkey_to_url(urlkey):
            try:
                # very rare bugged urlkeys appear
                domain, path = urlkey.split(')/', 1)
            except ValueError:
                return
            domain = domain.split(',')
            domain.reverse()
            domain = '.'.join(domain)
            if path:
                return '/'.join([domain, path])
            return domain

        urls_by_index = map(
            lambda ind: self.__get_domain_urls_in_index(ind, domain),
            self.indexes)
        all_urls = pipe(urls_by_index, concat, map(bytes.decode),
                        map(_urlkey_to_url), filter(None), map(unquote),
                        map(lambda x: x.strip()), unique, list)
        return all_urls

    def __get_domain_urls_in_index(self, index: str, domain: str) -> List[str]:
        pages_number = self._get_pages_number(index, domain)
        params = {
            'url': domain,
            'matchType': 'domain',
            'output': 'text',
            'filter': 'mime:text/html',
            'fl': 'urlkey'
        }
        resps = map(lambda page: self.__query_index(page, params, index),
                    range(pages_number))
        urls = pipe(resps, map(bytes.splitlines), concat)
        return urls

    def __query_index(self, page: int, params: Dict, index: str) -> bytes:
        """
        Query cc-index and retry when it fails with requests.exceptions.ChunkedEncodingError,
        why it occurs is beyond me.
        """
        try:
            resp = self.session.get(index, params={**params, 'page': page})
        except requests.exceptions.ChunkedEncodingError:
            time.sleep(random.randint(1, 4))
            return self.__query_index(page, params, index)
        return resp.content

    def _get_pages_number(self, index: str, domain: str) -> int:
        """
        Get number of pages in the index for the domain.
        """
        params = {
            'url': domain,
            'matchType': 'domain',
            'output': 'json',
            'showNumPages': 'true',
            'filter': 'mime:text/html'
        }
        resp = self.session.get(index, params=params)
        result = resp.json()
        return result['pages']

    def get_url_location(self, url: str) -> Optional[Dict]:
        """
        Get html location in index for url.
        """
        params = {
            'url': url,
            'output': 'json',
            'closest': self._cur_ts(),
            'filter': '!status:404',
            'fl': 'filename,length,offset,status,timestamp'
        }
        locations = pipe(self.indexes,
                         map(lambda index: self.__locate_url(index, params)),
                         filter(None), concat, list)
        if locations:
            location = self.__locate_most_relevant_location(locations)
            return location
        return None

    def __locate_url(self, index: str, params: Dict) -> Optional[Dict]:
        resp = self.session.get(index, params=params)
        if resp.status_code == 503:
            time.sleep(random.randint(1, 4))
            return self.__locate_url(index, params)
        elif resp.status_code in range(200, 300):
            content = resp.content.splitlines()
            results = map(json.loads, content)
            results = map(lambda x: {**x, 'index': index}, results)
            return results
        else:
            return None

    @staticmethod
    def __locate_most_relevant_location(locations: List[Dict]) -> Dict:
        """
        Find closest response with status 200, else any other response.
        """
        two_hundreds = [x for x in locations if x['status'] == '200']
        try:
            relevant = two_hundreds[0]
        except IndexError:
            relevant = locations[0]
        return relevant

    def get_page_data_from_warc(self, warc_filename: str, offset: int,
                                length: int) -> Dict:
        """
        Load html content from remote WARC file given
        offset and length.

        Parameters
        ----------
        warc_filename : str
            WARC filename.
        offset : int
            Starting position of html content in the archive.
        length : int
            Lenghth of the content

        Returns
        -------
        result : dict
            Dict warc header, http header, then raw html.
        """
        s3_file = self.bucket.Object(warc_filename)
        offset_end = offset + length - 1
        resp = s3_file.get(Range='bytes={}-{}'.format(offset, offset_end))

        content = gzip.decompress(resp['Body'].read()).decode(errors='replace')

        # content contains warc header, http header, then html itself
        # sometimes html is missing
        content_parst = content.strip().split('\r\n\r\n', 2)
        result = {}
        result['warc_header'] = content_parst[0]
        result['http_header'] = content_parst[1]
        try:
            result['html'] = content_parst[2]
        except IndexError:
            pass
        return result

    def load_page_data(self, url: str,
                       follow_redirect: bool = True) -> Optional[Dict]:
        """
        Load most recent html contents of the url.

        Parameters
        ----------
        url : str
            Target page url.
        follow_redirect : bool, default True
            If True, follow redirects. Number of redirects is limited to one.

        Returns
        -------
        result : dict
            Dictionary raw html of the page and index metadata.
            It has following fields:
                - index: Common Crawl index that contains the page
                - filename: path to the archive, containing the page
                - status: http status returned during crawl
                - offset: offset in bytes, indicates the place of the page
                          in the archive
                - length: length in bytes of the page in the archive
                - timestamp: time when the page was downloaded to the archive
                - warc_header: Common Crawl index metainfo for the page
                - http_header: http header returned by page server
                - html: raw html of the page
        """

        location = self.get_url_location(url)
        if location:
            offset = int(location['offset'])
            length = int(location['length'])
            warc_filename = location['filename']
            result = self.get_page_data_from_warc(warc_filename, offset,
                                                  length)

            if result:
                status = location['status']
                if follow_redirect and (status in ['301', '302']):
                    new_url = get_location_from_headers(result['http_header'])
                    if new_url:
                        new_result = self.load_page_data(new_url, False)
                        if new_result:
                            return new_result
                return {**location, **result}

        return location


def get_location_from_headers(headers: str) -> Optional[str]:
    """
    Parse raw HTTP headers and return them as a dictionary.

    Parameters
    ----------
    headers : str
        Raw HTTP headers.

    Returns
    -------
    parsed_headers : dict
        Parsed headers in a dictionary.
    """
    _, headers_alone = headers.split('\r\n', 1)
    parsed = email.message_from_file(io.StringIO(headers_alone))
    location = parsed.get('Location')
    if location:
        return str(location)
    return None
