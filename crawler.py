#!/usr/bin/env python
# encoding: utf-8

import sys
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
import time


class Crawler():

    def __init__(self, url, depth, db, key, logger, tp):
        self.url = url
        self.deep = depth
        self.cur_total = 1
        self.next_total = 0
        self.cur_level = 1
        self.db = db
        self.logger = logger
        self.pages = set()  # 集合用于网址去重
        self.table_name = '_' + re.search(r'^(?:https?://)(?:www.)?([^/]*)', url).groups(
            )[-1].replace('.', '_')  # 以起始网址命名数据表, 如"_sina_com_cn"
        self.db.create_table(self.table_name)
        self.key = key  # 抓取指定的关键字
        self.tp = tp

    def bfs_crawl(self, url, depth):
        if depth > self.deep:
            self.tp.jobs.task_done()
            sys.exit('fuck done')
        if depth > self.cur_level:
            with lock:
                if depth > self.cur_level:
                    print('\n' + '-' * 60)
                    self.tp.finished = 0
                    self.cur_total = self.next_total
                    self.cur_level = depth
                    if self.cur_level == depth:
                        self.next_total = 0
        try:
            if url not in self.pages:
                self.pages.add(url)
                self.logger.debug('fetch url:{}'.format(url))
                with lock:
                    self.tp.finished += 1
                    self.show_progress()
                    #sys.stdout.write('current level {}, finished {}, total {}\n'.format(self.cur_level, self.tp.finished, self.cur_total))
                    #sys.stdout.write(str(self.tp.jobs.unfinished_tasks))
                    #sys.stdout.write('\n')
                    new_urls = self.get_urls_from_url(url)
                    self.next_total += len(new_urls)
                for new_url in new_urls:
                    if new_url not in self.pages and depth<self.deep:
                        self.tp.add_job(self.bfs_crawl, new_url, depth+1)
        except TypeError as e:
            self.logger.error(e)
        except AttributeError as e:
            self.logger.error(e)
                
    def show_progress(self):
        sys.stdout.write(' ' * 60 + '\r')
        sys.stdout.flush()
        sys.stdout.write('level {}, finished {}, total {}\r'.format(
            self.cur_level, self.tp.finished,  self.cur_total))
        sys.stdout.flush()

    def get_urls_from_url(self, url):
        '''获取某页面的所有url '''
        if not url:
            return
        headers = {
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
        }
        try:
            response = requests.get(url, headers=headers, timeout=4)
        except requests.Timeout as e:
            self.logger.error(e)
            return
        except requests.ConnectionError as e:
            self.logger.error(e)
            return
        except requests.HTTPError as e:
            self.logger.error(e)
        except requests.RequestException as e:
            self.logger.error(e)

        html = response.content
        soup = self.parse_html(html)

        if self.key:
            if self.key in soup.text:
                self.save_content(url, content=html)
        else:
            self.save_content(url, content=html)

        links = []
        for link in soup.find_all('a', href=re.compile(r'^(https?|www).*$')):
            new_url = link.attrs['href']
            if new_url and new_url not in self.pages:
                links.append(new_url)
        return links

    def parse_html(self, html):
        html = html.decode('utf-8', 'ignore')
        try:
            soup = bs4.BeautifulSoup(html, 'lxml')
        except Exception as e:
            self.logger.info(e)
        return soup

    def save_content(self, url, content=None):
        try:
            lock.acquire()
            self.db.curs.execute("INSERT INTO {} (url, key, content) VALUES (:url, :key, :content)".format(self.table_name),
                                 {"url": url, "key": self.key, "content": content})
            self.db.conn.commit()
            self.logger.debug(
                'save content to database table: {}'.format(self.table_name))
        except sqlite3.Error as e:
            self.logger.critical(e)
            #raise e
        finally:
            lock.release()


class Database():

    def __init__(self, dbfile, logger):
        self.dbfile = dbfile
        self.logger = logger
        try:
            self.conn = sqlite3.connect(self.dbfile, check_same_thread=False)
            self.logger.debug(
                'connecting to sqlite3 database with db name {}'.format(self.dbfile))
            self.curs = self.conn.cursor()
        except sqlite3.Connection.Error as e:
            self.logger.critical(e)

    def create_table(self, table):
        #self.curs.execute("DROP TABLE IF EXISTS {}".format(table))
        try:
            self.curs.execute(
                'CREATE TABLE IF NOT EXISTS {} ( id INTEGER  PRIMARY KEY, key TEXT, url TEXT, content TEXT)'.format(table))
        except sqlite3.OperationalError as e:
            self.logger.error(e)
        except sqlite3.Error as e:
            self.logger.error(e)

"""
class Progress(threading.Thread):
    def __init__(self,crawler, tp):
        super().__init__()
        self.c = crawler
        self.tp = tp
        self.daemon = True
        #self.start()

    def run(self):
        while True:
            self.show_progress()
            time.sleep(5)

    def show_progress(self):
        sys.stdout.write(' ' * 60 + '\r')
        sys.stdout.flush()
        sys.stdout.write('level {}, finished {}, total {}\r'.format(
            self.c.cur_level, self.tp.finished,  self.c.cur_total))
        sys.stdout.flush()
"""
        

class Worker(threading.Thread):

    def __init__(self, threadpool):
        super().__init__()
        self.tp = threadpool
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tp.jobs.get()
            func(*args, **kargs)
            self.tp.jobs.task_done()
            #    self.tp.finished += 1


class Threadpool():

    def __init__(self, jobs_num):
        self.finished = 0
        self.jobs_num = jobs_num
        #self.threads_num = threads_num
        #self.threads = []
        self.jobs = queue.Queue()

        for i in range(self.jobs_num):
            Worker(self)

    def add_job(self, func, *args, **kargs):
        self.jobs.put((func, args, kargs))

    def wait_completion(self):
        self.jobs.join()


def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store',
                      default='http://www.sina.com.cn', help='specify the url')
    parser.add_option('-d', '--depth', dest='depth', action='store',
                      type='int', default=5, help='specify the crawl depth')
    parser.add_option('-f', '--logfile', dest='logfile', action='store',
                      default='spider.log', help='specify log file ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store',
                      type='int', default=5, help='specify log level, default 1')
    parser.add_option('-k', '--key', dest='key',
                      action='store', help='specify the keyword')
    parser.add_option('-t', '--thread', dest='thread', action='store',
                      type='int', default=5, help='multi threading')
    parser.add_option('--dbfile', dest='dbfile', action='store',
                      default='spider.db', help='database file')
    parser.add_option('--test', '--testself', dest='testself',
                      action='store_true', default=False, help='program test self')
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
    # logger.addHandler(logging.NullHandler())
    form = logging.Formatter("%(levelname)-10s %(asctime)s %(message)s")
    handler = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=2048000, backupCount=3)
    handler.setFormatter(form)
    logger.addHandler(handler)
    return logger

if __name__ == '__main__':

    lock = threading.RLock()
    opt = get_options()
    logger = get_a_logger(opt.logfile, opt.loglevel)
    db = Database(opt.dbfile, logger)
    tp = Threadpool(opt.thread)
    c = Crawler(opt.url, opt.depth, db, opt.key, logger, tp)
    #progress = Progress(c,tp)
    #progress.start()
    tp.add_job(c.bfs_crawl, opt.url, 1)
    tp.wait_completion()
    print('well  done')
