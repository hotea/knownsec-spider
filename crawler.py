#!/usr/bin/env python
# encoding: utf-8

import urllib.request
import requests
import bs4
import re
import sqlite3
import optparse
import logging

class Crawler():
    def __init__(self, url, depth=1):
        self.url = url
        self.depth = depth
        self.pages = set()

    def is_crawled(self, url):
        return True if url in self.pages else False

    def get_html(self, url, depth):
        if depth > 0:
            headers = {
		'Connection': 'keep-alive',
		'Accept': 'text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8',
		'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
		'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
	    }
#            req = urllib.request.Request(url, headers=headers)
            try:
                #html = urllib.request.urlopen(req, timeout=3)
                response = requests.get(url, headers=headers,timeout=2) 
            except requests.Timeout as e:
                print('Timeout Error: ', e)
                return
            except requests.ConnectionError as e:
                print('ConnectionError Error: ', e)
                return
            except requests.HTTPError as e:
                print('HTTPError Error: ', e)
            except requests.RequestException  as e:
                print('Requestexception Error happend:', e)

            html = response.text 
            for new_url in self.parse_html(html):
                    print(new_url)
                    # 深度优先爬取
                    self.get_html(new_url, depth-1)


    def parse_html(self, html): 
        links = []
        soup = bs4.BeautifulSoup(html, 'lxml')
        for link in soup.find_all('a', href=re.compile(r'^(https?|www).*$')):
            new_url = link.attrs['href']
            if new_url  and not self.is_crawled(new_url):
                self.pages.add(new_url)
                links.append(new_url)
        return links

class Database():
    def __init__(self, dbfile):
        self.conn = sqlite3.connect(dbfile)
        


def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store', default='http://www.sina.com.cn', help='specify the url')
    parser.add_option('-d', '--depth', dest='depth', action='store', type='int', default=1, help='specify the crawl depth')
    parser.add_option('-f', '--logfile', dest='logfile', action='store', default='spider.log', help='specify log file ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store', type='int', default=1, help='specify log level, default 1')
    return parser.parse_args()[0]

if __name__ == '__main__':

    opt = get_options()
    c = Crawler(opt.url, opt.depth)
    c.get_html(c.url, c.depth)
        


