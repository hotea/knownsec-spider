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
from collections import deque

pages = set()

class Crawler():
    def __init__(self, url, depth, db, key, logger, tp):
        self.tp = tp
        self.url = url
        self.depth = depth
        self.db = db
        self.logger = logger
        global pages
        self.pages = pages  #集合用于网址去重
        self.table_name = '_' + re.search(r'^(?:https?://)(?:www.)?([^/]*)', url).groups()[-1].replace('.', '_')  #以起始网址命名数据表, 如"_sina_com_cn"
        self.db.create_table(self.table_name)
        self.key = key  # 抓取指定的关键字


    def dfs_crawl(self, url, depth, key=None):
        if depth > 0:
            if not url: return
            self.pages.add(url)
            try:
                for new_url in self.get_urls_from_url(url, key):
                    # 深度优先爬取
                    if new_url not in self.pages:
                        print(new_url)
                        self.logger.debug('found url:' + new_url)
                        self.tp.add_job(self.dfs_crawl, new_url, depth-1, key)
            except TypeError as e:
                self.logger.info(e)
                pass
        

    def get_urls_from_url(self, url, key):
        if not url: return
        '''获取页面, 爬虫的主要功能'''
        headers = {
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
        }
        try:
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
             
        links = [] 
        for link in soup.find_all('a', href=re.compile(r'^(https?|www).*$')):
            new_url = link.attrs['href']
            if new_url  and new_url not in self.pages:
                links.append(new_url)
        return links


    def parse_html(self, html):
        #html = html.decode()
        try:
            soup = bs4.BeautifulSoup(html, 'lxml')
        except Exception as e:
            self.logger.warning(e)
        return soup

    
    def save_content(self, url, key=None, content=None):
        try:
            lock.acquire()
            self.db.curs.execute("INSERT INTO {} (url, key, content) VALUES (:url, :key, :content)".format( self.table_name),
                    {"url": url, "key": self.key, "content": content})
            self.db.conn.commit()
            self.logger.debug('save content to database table: {}'.format(self.table_name))
        except sqlite3.Error as e:
            self.logger.critical(e)
            raise e
        finally:
            lock.release()


class Database():
    def __init__(self, dbfile, logger):
        self.dbfile = dbfile
        self.logger = logger
        try:
            self.conn = sqlite3.connect(self.dbfile, check_same_thread=False)
            self.logger.debug('connecting to sqlite3 database with db name {}'.format(self.dbfile))
            self.curs = self.conn.cursor()
        except sqlite3.Connection.Error as e:
            self.logger.critical(e)
        

    def create_table(self, table):
        #self.curs.execute("DROP TABLE IF EXISTS {}".format(table))
        try:
            self.curs.execute('CREATE TABLE IF NOT EXISTS {} ( id INTEGER  PRIMARY KEY, key TEXT, url TEXT, content TEXT)'.format(table))
        except sqlite3.OperationalError as e:
            self.logger.error(e)
        except sqlite3.Error as e:
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
            func(*args, **kargs)
            self.jobs.task_done()

class Threadpool():
    def __init__(self, jobs_num):
        self.jobs_num = jobs_num
        #self.threads_num = threads_num
        self.jobs = queue.Queue()
        for i in range(self.jobs_num):
            Worker(self.jobs)
        #self.threads = []
        #self._init_pool(self.threads_num)
    def add_job(self, func, *args, **kargs):
        self.jobs.put((func, args, kargs))

    '''
    def init_job_queue(self, func, *args, **kargs):
        for i in range(self.jobs_num):
            self.add_job((func, args, kargs))
    '''
    def wait_completion(self):
        self.jobs.join()



def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store', default='http://www.sina.com.cn', help='specify the url')
    parser.add_option('-d', '--depth', dest='depth', action='store', type='int', default=5, help='specify the crawl depth')
    parser.add_option('-f', '--logfile', dest='logfile', action='store', default='spider.log', help='specify log file ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store', type='int', default=5, help='specify log level, default 1')
    parser.add_option('-k', '--key', dest='key', action='store', help='specify the keyword')
    parser.add_option('-t', '--thread', dest='thread', action='store', type='int', default=5, help='multi threading')
    parser.add_option('--dbfile', dest='dbfile', action='store', default='spider.db', help='database file')
    parser.add_option('--test', '--testself', dest='testself', action='store_true', default=False, help='program test self')
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
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=2048000, backupCount=3)
    handler.setFormatter(form)
    logger.addHandler(handler)
    return logger

if __name__ == '__main__':

    lock = threading.RLock()
    opt = get_options()
    logger = get_a_logger(opt.logfile, opt.loglevel)
    db = Database(opt.dbfile, logger)
    tp = Threadpool(opt.thread )
    c = Crawler(opt.url, opt.depth, db, opt.key, logger, tp)
#    c.dfs_crawl(c.url, c.depth, c.key)
#    tp.init_job_queue(c.dfs_crawl, c.url, c.depth, c.key)
    tp.add_job(c.dfs_crawl, c.url, c.depth, c.key)
    tp.wait_completion()
        


