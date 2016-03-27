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
    def __init__(self, url, depth, db, key):
        self.url = url
        self.depth = depth
        self.db = db
        self.pages = set()
        self.table_name = re.search(r'^((https?://)?(www.)?)(.*)$', url).groups()[-1].replace('.', '_')
        self.db.create_table(self.table_name)
        self.key = key

    def is_crawled(self, url):
        return True if url in self.pages else False

    def get_html(self, url, depth, key=None):
        if depth > 0:
            headers = {
		'Connection': 'keep-alive',
		'Accept': 'text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8',
		'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
		'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
	    }
            try:
                self.pages.add(url)
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

            html = response.content 
            soup = self.parse_html(html)
            
            if key:
                if key in soup.text:
                    self.save_content(url, key, content=html)
            else:
                self.save_content(url, content=html)

            for new_url in self.parse_new_url(soup):
                print(new_url)
                # 深度优先爬取
                self.get_html(new_url, depth-1, key)


    def parse_html(self, html):
        #html = html.decode()
        soup = bs4.BeautifulSoup(html, 'lxml')
        return soup

    def parse_new_url(self, soup): 
        #links = []
        links = set() 
        for link in soup.find_all('a', href=re.compile(r'^(https?|www).*$')):
            new_url = link.attrs['href']
            if new_url  and not self.is_crawled(new_url):
                #self.pages.add(new_url)
                links.add(new_url)
        return links
    
    def save_content(self, url, key=None, content=None):
        self.db.curs.execute("INSERT INTO {} (url, key, content) VALUES (:url, :key, :content)".format( self.table_name),
                {"url": url, "key": self.key, "content": content})
        self.db.conn.commit()


class Log():
    def __init__(self):
        pass

class Database():
    def __init__(self, dbfile):
        self.conn = sqlite3.connect(dbfile)
        self.curs = self.conn.cursor()
        

    def create_table(self, table):
        #self.curs.execute("DROP TABLE IF EXISTS {}".format(table))
        self.curs.execute('CREATE TABLE IF NOT EXISTS {} ( id INTEGER  PRIMARY KEY, key TEXT, url TEXT, content TEXT)'.format(table))





def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store', default='http://www.sina.com.cn', help='specify the url')
    parser.add_option('-d', '--depth', dest='depth', action='store', type='int', default=1, help='specify the crawl depth')
    parser.add_option('-f', '--logfile', dest='logfile', action='store', default='spider.log', help='specify log file ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store', type='int', default=1, help='specify log level, default 1')
    parser.add_option('-k', '--key', dest='key', action='store', help='specify the keyword')
    parser.add_option('--dbfile', dest='dbfile', action='store', default='spider.db', help='database file')
    parser.add_option('--test', '--testself', dest='testself', action='store_true', default=False, help='program test self')
    parser.add_option('-t', '--thread', dest='thread', action='store', type='int', default=2, help='multi threading')

    return parser.parse_args()[0]

if __name__ == '__main__':

    opt = get_options()
    db = Database(opt.dbfile)
    c = Crawler(opt.url, opt.depth, db, opt.key)
    c.get_html(c.url, c.depth, c.key)
        


