#!/usr/bin/env python
# encoding: utf-8

import urllib.request
import requests
import bs4
import re
import sqlite3
import optparse
import logging
import logging.handlers
import threading
import queue


class Crawler():
    def __init__(self, url, depth, db, key, logger):
        self.url = url
        self.depth = depth
        self.db = db
        self.logger = logger
        self.pages = set()
        self.table_name = '_' + re.search(r'^((https?://)?(www.)?)(.*)$', url).groups()[-1].replace('.', '_')
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
                self.logger.error(e)
                return
            except requests.ConnectionError as e:
                self.logger.error(e)
                return
            except requests.HTTPError as e:
                self.logger.error(e)
            except requests.RequestException  as e:
                self.logger.error(e)

            html = response.content 
            soup = self.parse_html(html)
            
            if key:
                if key in soup.text:
                    self.save_content(url, key, content=html)
            else:
                self.save_content(url, content=html)

            for new_url in self.parse_new_url(soup):
                print(new_url)
                self.logger.debug(new_url)
                # 深度优先爬取
                self.get_html(new_url, depth-1, key)


    def parse_html(self, html):
        #html = html.decode()
        try:
            soup = bs4.BeautifulSoup(html, 'lxml')
        except:
            raise
        return soup

    def parse_new_url(self, soup): 
        links = set() 
        for link in soup.find_all('a', href=re.compile(r'^(https?|www).*$')):
            new_url = link.attrs['href']
            if new_url  and not self.is_crawled(new_url):
                links.add(new_url)
        return links
    
    def save_content(self, url, key=None, content=None):
        self.db.curs.execute("INSERT INTO {} (url, key, content) VALUES (:url, :key, :content)".format( self.table_name),
                {"url": url, "key": self.key, "content": content})
        self.db.conn.commit()

class Database():
    def __init__(self, dbfile, logger):
        self.dbfile = dbfile
        self.logger = logger
        self.conn = sqlite3.connect(self.dbfile, check_same_thread=False)
        self.curs = self.conn.cursor()
        

    def create_table(self, table):
        #self.curs.execute("DROP TABLE IF EXISTS {}".format(table))
        try:
            self.curs.execute('CREATE TABLE IF NOT EXISTS {} ( id INTEGER  PRIMARY KEY, key TEXT, url TEXT, content TEXT)'.format(table))
        except sqlite3.OperationalError as e:
            self.logger.error(e)

class Worker(threading.Thread):
    def __init__(self, jobs):
        super().__init__()
        self.jobs = jobs
        self.daemon = True
        self.start()
    def run(self):
        while True:
            func, args, kargs = self.jobs.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                raise e
            self.tasks.task_done()

class Threadpool():
    def __init__(self, jobs_num):
        self.jobs_num = jobs_num
        #self.threads_num = threads_num
        self.jobs = queue.Queue()
        for i in range(self.jobs_num):
            Worker(self.jobs)
        #self.threads = []
        #self._init_pool(self.threads_num)


    def init_job_queue(self, func, *args, **kargs):
        for i in range(self.jobs_num):
            self.jobs.put((func, args, kargs))
    def wait_completion(self):
        self.jobs.join()



def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store', default='http://www.sina.com.cn', help='specify the url')
    parser.add_option('-d', '--depth', dest='depth', action='store', type='int', default=5, help='specify the crawl depth')
    parser.add_option('-f', '--logfile', dest='logfile', action='store', default='spider.log', help='specify log file ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store', type='int', default=5, help='specify log level, default 1')
    parser.add_option('-k', '--key', dest='key', action='store', help='specify the keyword')
    parser.add_option('--dbfile', dest='dbfile', action='store', default='spider.db', help='database file')
    parser.add_option('--test', '--testself', dest='testself', action='store_true', default=False, help='program test self')
    parser.add_option('-t', '--thread', dest='thread', action='store', type='int', default=5, help='multi threading')
    return parser.parse_args()[0]

def get_a_logger(logfile, loglevel):
    LEVELS = {
            1: logging.CRITICAL,
            2: logging.ERROR,
            3: logging.WARNING,
            4: logging.INFO,
            5: logging.DEBUG,
            }
    logger = logging.getLogger(__name__)
    logger.setLevel(LEVELS.get(loglevel))
    #logger.addHandler(logging.NullHandler())
    form = logging.Formatter("%(levelname)-10s %(asctime)s %(message)s")
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=20480000, backupCount=3)
    handler.setFormatter(form)
    logger.addHandler(handler)
    return logger

if __name__ == '__main__':

    opt = get_options()
    logger = get_a_logger(opt.logfile, opt.loglevel)
    db = Database(opt.dbfile, logger)
    c = Crawler(opt.url, opt.depth, db, opt.key, logger)
    #c.get_html(c.url, c.depth, c.key)
    tp = Threadpool(opt.thread)
    tp.init_job_queue(c.get_html, c.url, c.depth, c.key)
    tp.wait_completion()
        


