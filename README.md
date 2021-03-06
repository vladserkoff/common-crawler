# Common Crawler

An app that lets you find and download web pages contents from [common crawl](http://commoncrawl.org/).

## Instalation

`pip install git+https://github.com/vladserkoff/common-crawler.git`

## Usage

### (Optional) Deploy Common Crawl index server

Best practice is to deploy your own index server as to not overuse the server hosted by Common Crawl.

```bash
# deploy local common crawl index
git clone https://github.com/commoncrawl/cc-index-server.git
cd cc-index-server
# edit install-collections.sh to only include recent indexes, otherwise it will load gigabytes of data.
docker build -t cc-index-server .
docker run -d -p 8080:8080 cc-index-server
```

### Find available urls for a domain, then load an html with additional metadata

```python
In [1]: from common_crawler import CommonCrawler

In [2]: cc = CommonCrawler('http://localhost:8080') # or leave it blank to use Common Crawl's server.

In [3]: urls = cc.find_domain_urls('http://example.com')

In [4]: len(urls)
Out[4]: 2958

In [5]: dat = cc.load_page_data(urls[0])

In [6]: dat.keys()
Out[6]: dict_keys(['filename', 'length', 'offset', 'status', 'timestamp', 'index', 'warc_header', 'http_header', 'html'])
```
