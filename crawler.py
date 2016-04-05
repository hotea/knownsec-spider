#!/usr/bin/env python
# _*_ encoding: utf-8 _*_

# __author__ = 'sukai@outlook.com'
# __version__ = '0.99'


import bs4
import doctest
import logging
import logging.handlers
import optparse
import queue
import re
import requests
import sqlite3
import sys
import threading
import time


class Crawler():

    def __init__(self, url, depth_limit, db, key, logger, tp, lock):
        self.url = url  # 入口URL
        self.depth_limit = depth_limit  # 深度限制
        self.key = key  # 抓取指定的关键字
        self.logger = logger    # 日志实例
        self.tp = tp    # 线程池实例
        self.lock = lock
        self.pages = set()  # 集合用于网址去重
        self.db = db # 数据库实例
        self.table_name = '_' + re.search(r'^(?:https?://)(?:www.)?([^/]*)', url).groups(
            )[-1].replace('.', '_')  # 以起始网址命名数据表, 如"_sina_com_cn"
        self.db.create_table(self.table_name)
        self.cur_level = 1 # 当前所处的层
        self.cur_total = 1 # 当前层的URL总数
        self.next_total = 0 # 下层的URL总数
        self.failed_total = 0 # 访问失败的URL总数

    def crawl(self, url, depth):
        '''实现爬取功能的主要函数'''

        if depth > self.depth_limit: # 当前深度超过指定深度, 停止抓取, 将工作队列标记为任务完成
            self.db.conn.commit()
            self.logger.info(
                'save content to database table: {}'.format(self.table_name))
            self.tp.jobs.task_done()
        if depth > self.cur_level: # 进入下一层
            time.sleep(1 * self.cur_level * self.tp.thread_num) # 当切换层数时, 用sleep来简化同步, 较早到达下层的线程等待一段时间,
                                                                # 使所有线程都抵达下一层, 然后再更新下层任务数
            with self.lock:
                if depth > self.cur_level: # 双重检查锁, 保证该语句块只被多个线程中的一个执行一次
                    self.logger.info('level {} finished, total URL number: {}, failed {}'.format(
                        self.cur_level, self.cur_total, self.failed_total))
                    print('\n' + '=' * 60)
                    self.tp.task_finished = 0 # 将已完成任务数重置为0
                    self.cur_total = self.next_total # 设置新一层的任务总数, 此数目由爬取完上层URL之后得来
                    #self.cur_total = self.tp.jobs.unfinished_tasks
                    self.cur_level = depth
                    self.next_total = 0
                    self.logger.info('enter the {} level'.format(self.cur_level))
                    self.logger.info('the URL numbers of the {} level is {}'.format(self.cur_level, self.cur_total))
            return
        try:
            if  url not in self.pages:
                self.pages.add(url) # 将新抓取的URL放入重复检测集合
                self.logger.debug('fetch url: {}'.format(url))
                new_urls = self.get_urls_from_url(url) # 获取此页面链出的URL
                if not new_urls:
                    self.failed_total += 1
                    return
                #with self.lock:
                if depth == self.depth_limit: # 如果该层是最后一层则不需要添加新任务, 直接返回
                    return
                self.next_total += len(new_urls)
                for new_url in new_urls: # 将每一个新解析出的URL加入任务, 并更新下层任务总数
                    #self.next_total += 1
                    self.tp.add_job(self.crawl, new_url, depth+1)
        except Exception as e:
            self.logger.critical(e)
            raise e

    def get_urls_from_url(self, url):
        '''获取某页面的所有url '''
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml, application/xml;q=0.9',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive',
        }
        try:
            response = requests.get(url, headers=headers, timeout=2)
        except requests.Timeout as e:
            self.logger.error(e)
            return
        except requests.ConnectionError as e:
            self.logger.error(e)
            return
        except requests.HTTPError as e:
            self.logger.error(e)
            return
        except requests.RequestException as e:
            self.logger.error(e)
            return

        html = response.content
        soup = self.parse_html(html)

        # 如果指定了关键字, 则只保存带关键字的网页, 否则保存所有网页
        if self.key:
            if self.key in soup.text:
                self.save_content(url, content=html)
        else:
            self.save_content(url, content=html)

        #  查找所有新的URL并保存在列表返回
        links = []
        for link in soup.find_all('a', href=re.compile(r'^(https?|www).*$')):
            new_url = link.attrs['href']
            if new_url and new_url not in self.pages:
                links.append(new_url)
        return links

    def parse_html(self, html):
        '''解析获取的页面, 返回BeautifulSoup对象'''
        html = html.decode('utf-8', 'ignore')
        try:
            soup = bs4.BeautifulSoup(html, 'lxml')
        except Exception as e:
            self.logger.error('parse error: {}'.format(e))
        return soup

    def save_content(self, url, content=None):
        '''保存信息到数据库'''
        try:
            self.lock.acquire()
            self.db.curs.execute("INSERT INTO {} (url, key, content) VALUES (:url, :key, :content)".format(self.table_name),
                                 {"url": url, "key": self.key, "content": content})
            self.db.conn.commit()
            self.logger.info(
                'save content to database table: {}'.format(self.table_name))
        except sqlite3.Error as e:
            self.logger.critical('sqlite3 error: {}'.format(e))
            return
        finally:
            self.lock.release()


class Database():

    def __init__(self, dbfile, logger):
        self.dbfile = dbfile # 指定数据库文件
        self.logger = logger
        try:
            self.conn = sqlite3.connect(self.dbfile, check_same_thread=False)
            self.logger.info(
                'connecting to sqlite3 database with db name {}'.format(self.dbfile))
            self.curs = self.conn.cursor()
        except sqlite3.Connection.Error as e:
            self.logger.critical('sqlite3 error: {}'.format(e))
            return
        except sqlite3.Error as e:
            self.logger.critical('sqlite3 error: {}'.format(e))
            return

    def create_table(self, table):
        '''创建数据表'''
        try:
            self.curs.execute(
                'CREATE TABLE IF NOT EXISTS {} ( id INTEGER  PRIMARY KEY, key TEXT, url TEXT, content TEXT)'.format(table))
        except sqlite3.OperationalError as e:
            self.logger.error(e)
        except sqlite3.Error as e:
            self.logger.error(e)

class Progress(threading.Thread):
    '''进度类'''
    def __init__(self,crawler, tp):
        super().__init__()
        self.c = crawler
        self.tp = tp
        self.daemon = True
        self.start()

    def run(self):
        while True:
            self.show_progress()
            time.sleep(1)

    def show_progress(self):
        '''显示进度的函数'''
        sys.stdout.write(' ' * 60 + '\r')
        sys.stdout.flush()
        sys.stdout.write('当前层: {}, 该层URL总数: {}, 已完成数量: {}, 失败数量: {}, 进度: {:.2f}%\r'.format(
            self.c.cur_level, self.c.cur_total, self.tp.task_finished,
            self.c.failed_total, self.tp.task_finished / self.c.cur_total * 100))
        sys.stdout.flush()

class Worker(threading.Thread):
    '''被放入线程池中的工作线程'''

    def __init__(self, threadpool):
        super().__init__()
        self.tp = threadpool
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tp.jobs.get() # 获取一个任务
            func(*args, **kargs)
            self.tp.task_finished += 1
            self.tp.jobs.task_done() # 通知任务队列该任务完成


class Threadpool():
    '''线程池'''

    def __init__(self, thread_num):
        self.thread_num = thread_num 
        self.jobs = queue.Queue() # 工作队列
        self.threads = [] # 线程池
        self.task_finished = 0
        self.__init_threadpool(thread_num)
    def __init_threadpool(self, thread_num):
        for i in range(thread_num):
            self.threads.append(Worker(self))

    def add_job(self, func, *args, **kargs):
        '''将任务放入队列'''
        self.jobs.put((func, args, kargs))

    def wait_completion(self):
        self.jobs.join()


def get_options():
    '''解析命令行选项'''
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store',
                       help='指定起始URL')
    parser.add_option('-d', '--depth', dest='depth', action='store',
                      type='int', default=3, help='指定爬取深度, 默认3层')
    parser.add_option('-f', '--logfile', dest='logfile', action='store',
                      default='spider.log', help='指定日志文件, 默认spider.log ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store',
                      type='int', default=3, help='指定日志级别, 默认ERROR级')
    parser.add_option('-k', '--key', dest='key',
                      action='store', help='指定关键词, 如未指定则保存所有网页')
    parser.add_option('-t', '--thread', dest='thread', action='store',
                      type='int', default=10, help='指定线程池大小, 默认10个线程')
    parser.add_option('--dbfile', dest='dbfile', action='store',
                      default='spider.db', help='指定数据库文件, 默认 spider.db')
    parser.add_option('--test', '--testself', dest='testself',
                      action='store_true', default=False, help='程序是否自测, 默认否')
    return parser.parse_args()[0]


def get_a_logger(logfile, loglevel):
    '''返回一个日志记录器实例'''
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
    form = logging.Formatter("%(levelname)-8s %(asctime)s %(message)s") # 日志显示格式
    handler = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=2048000, backupCount=3) # 设置为旋转日志
    handler.setFormatter(form)
    logger.addHandler(handler)
    return logger

def main():
    '''
    '''
    lock = threading.RLock() # 线程锁
    opt = get_options()
    if opt.testself:
        import doctest
        print(doctest.testmod())
        sys.exit('Selftest finished\n')
    if not opt.url:
        sys.exit('Need to specify the URL, use "-h" see help\n')
    logger = get_a_logger(opt.logfile, opt.loglevel)
    db = Database(opt.dbfile, logger)
    tp = Threadpool(opt.thread)
    c = Crawler(opt.url, opt.depth, db, opt.key, logger, tp, lock)
    progress = Progress(c, tp)
    tp.add_job(c.crawl, opt.url, 1) # 将起始URL加入任务队列, 设置当前深度为1
    logger.info('tasks start!, destination url: {}, depth limitation: {}'.format(opt.url, opt.depth))
    try:
        tp.wait_completion() # 等待所有任务完成
        logger.info('all tasks done')
    except KeyboardInterrupt:  # 按 Ctrl+C 退出
        db.conn.commit()
        logger.info('save content to database table: {}'.format(c.table_name))
        logger.info('task canceled by user')
        sys.exit('\n任务取消, 退出... I will be back!\n')
    logger.info('exit normally')
    sys.exit('\n抓取完成')

if __name__ == '__main__':
    main()
