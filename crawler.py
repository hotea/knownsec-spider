#!/usr/bin/env python
# _*_ encoding: utf-8 _*_

# __author__ = 'sukai@outlook.com'
# __version__ = '0.99a'
# with disk queue


import bs4
import doctest
import logging
import logging.handlers
import optparse
import pickle
import queue
import queuelib
import re
import requests
import shutil
import sqlite3
import sys
import threading
import traceback
import time


class Crawler():
    '''爬虫类'''

    def __init__(self, url, depth_limit, db, key, tp):
        self.url = url  # 入口URL
        self.depth_limit = depth_limit  # 深度限制
        self.key = key  # 抓取指定的关键字
        self.tp = tp    # 线程池实例
        self.pages = set()  # 集合用于网址去重
        self.db = db  # 数据库实例
        self.table_name = '_' + re.search(r'^(?:https?://)(?:www.)?([^/]*)', url).groups(
        )[-1].replace('.', '_')  # 以起始网址命名数据表, 如"_sina_com_cn"
        self.db.create_table(self.table_name)
        self.current_level = 1  # 当前所处的层
        self.current_total = 1  # 当前层的URL总数
        self.next_total = 0  # 下层的URL总数
        self.current_failed = 0  # 当前层访问失败的URL总数
        self.total = 0  # 所有层的URL总数, 用于最后打印报告
        self.total_failed = 0  # 所有层失败URL的总数
        self.total_finished = 0  # 所有层完成URL的总数, 在正常退出而非用户中断的情况下, 此数据等于 self.total

    def crawl(self, url, depth):
        '''实现爬取功能的主要函数'''

        if depth > self.depth_limit:  # 当前深度超过指定深度, 停止抓取, 将工作队列标记为任务完成
            self.db.conn.commit()
            logger.debug(
                'save content to database table: {}'.format(self.table_name))
            self.tp.job.task_done()  # 通知队列任务完成
        if depth > self.current_level:  # 进入下一层
            # 当切换层数时, 用sleep来简化同步, 较早到达下层的线程等待一段时间,
            # 使所有线程都抵达下一层, 然后再更新下层任务数, 保证数目正确
            time.sleep(1 * self.current_level * self.tp.thread_num)
            with lock:
                if depth > self.current_level:  # 双重检查锁, 保证该语句块只被多个线程中的一个执行一次
                    self.change_level(depth)  # 切换层数
            return
        try:
            if url not in self.pages:
                self.pages.add(url)  # 将新抓取的URL放入重复检测集合
                logger.debug('fetch url: {}'.format(url))
                new_urls = self.get_urls_from_url(url)  # 获取此页面链出的URL
                if not new_urls:
                    self.current_failed += 1
                    return
                if depth == self.depth_limit:  # 如果该层是最后一层则不需要添加新任务, 直接返回
                    return
                self.next_total += len(new_urls)  # 更新下层任务总数
                for new_url in new_urls:  # 将每一个新解析出的URL加入任务
                    self.tp.add_job(self.crawl, new_url, depth + 1)
        except Exception as e:
            logger.critical(e)
            logger.critical(traceback.format_exc())

    def change_level(self, depth):
        '''进入下一层'''
        logger.info('level {} finished, total URL number: {}, failed {}'.format(
            self.current_level, self.current_total, self.current_failed))
        print('\n' + '=' * 60)
        self.total_finished += self.tp.current_finished  # 更新完成总数
        self.tp.current_finished = 0  # 当前层已完成任务数重置为0
        self.total_failed += self.current_failed  # 更新失败总数
        self.current_failed = 0  # 当前层失败数置0
        self.total += self.current_total  # 更新URL总数
        self.current_total = self.next_total  # 设置新一层的任务总数, 此数目由爬取完上层URL之后得来
        self.next_total = 0  # 下层总数置0
        self.current_level = depth  # 当前层编号设置为下层编号
        logger.info('enter the {} level'.format(self.current_level))
        logger.info('the URL numbers of the {} level is {}'.format(
            self.current_level, self.current_total))

    def get_urls_from_url(self, url):
        '''获取某页面的所有url '''
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml, application/xml;q=0.9',
            'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive',
        }
        try:
            # 可视网络情况调整 timeout 时长
            response = requests.get(url, headers=headers, timeout=2)
        except requests.Timeout as e:
            logger.error(e)
            return
        except requests.ConnectionError as e:
            logger.error(e)
            return
        except requests.HTTPError as e:
            logger.error(e)
            return
        except requests.RequestException as e:
            logger.error(e)
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
        '''解析获取的页面, 返回BeautifulSoup对象 '''
        html = html.decode('utf-8', 'ignore')
        try:
            soup = bs4.BeautifulSoup(html, 'lxml')
        except Exception as e:
            logger.error('parse error: {}'.format(e))
        return soup

    def save_content(self, url, content=None):
        '''保存信息到数据库 '''
        try:
            lock.acquire()
            self.db.curs.execute("INSERT INTO {} (url, key, content) VALUES (:url, :key, :content)".format(self.table_name),
                                 {"url": url, "key": self.key, "content": content})
            self.db.conn.commit()
            logger.debug(
                'save content to database table: {}'.format(self.table_name))
        except sqlite3.Error as e:
            logger.critical('sqlite3 error: {}'.format(e))
            logger.critical(traceback.format_exc())
        finally:
            lock.release()

    def report(self):
        '''最后的统计报告'''
        print('\n', '-' * 30, ' 结果统计 ', '-' * 30)
        print('总URL数: {}, 共抓取: {}, 其中失败: {}'.format(
            self.total, self.total_finished, self.total_failed))
        try:
            num = self.db.curs.execute(
                "SELECT COUNT(*) AS NUM FROM {}".format(self.table_name))
            print('共保存了 {} 个页面'.format(num.fetchone()[0]))
            self.db.conn.close()
        except sqlite3.Error as e:
            logger.error(e)
            return


class Database():
    '''数据库类'''

    def __init__(self, dbfile):
        self.dbfile = dbfile  # 指定数据库文件
        self._init_database()

    def _init_database(self):
        try:
            self.conn = sqlite3.connect(self.dbfile, check_same_thread=False)
            logger.info(
                'connecting to sqlite3 database with db name {}'.format(self.dbfile))
            self.curs = self.conn.cursor()
        except sqlite3.Connection.Error as e:
            logger.critical('sqlite3 error: {}'.format(e))
            logger.critical(traceback.format_exc())
            return
        except sqlite3.Error as e:
            logger.critical('sqlite3 error: {}'.format(e))
            logger.critical(traceback.format_exc())
            return

    def create_table(self, table):
        '''创建数据表'''
        try:
            self.curs.execute(
                'CREATE TABLE IF NOT EXISTS {} ( id INTEGER  PRIMARY KEY, key TEXT, url TEXT, content TEXT)'.format(table))
        except sqlite3.OperationalError as e:
            logger.error(e)
        except sqlite3.Error as e:
            logger.error(e)


class Progress(threading.Thread):
    '''进度类'''

    def __init__(self, crawler, tp):
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
        sys.stdout.write('当前层: {}, 该层URL总数: {}, 已完成: {}, 失败: {}, 进度: {:.2f}%\r'.format(
            self.c.current_level, self.c.current_total, self.tp.current_finished,
            self.c.current_failed, self.tp.current_finished / self.c.current_total * 100))
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
            func, args = self.tp.job.get()  # 获取一个任务
            logger.debug('get a task from job queue, job size: {}'.format(self.tp.job.qsize()))
            func(*args)  # 执行该任务
            self.tp.current_finished += 1  # 更新完成任务数
            self.tp.job.task_done()  # 通知任务队列该任务完成

            # 如果队列满事件已经置位, 且队列已空, 则将磁盘队列job2的任务移到队列job
            if self.tp.job_full_event.is_set() and self.tp.job.qsize() == 0: 
                try:
                    lock.acquire()
                    r = min(self.tp.q_upper_limit, self.tp.job2.info['size'])
                    for _ in range(r):
                        self.tp.job.put((func, pickle.loads(self.tp.job2.pop())))
                    logger.debug('move tasks from job2 to job, then job size is {}, job2 size is{}'.format(self.tp.job.qsize(), self.tp.job2.info['size']))
                    if self.tp.job2.info['size'] == 0:
                        logger.info('!!! job2 empty, looks good !!!')
                        self.tp.job_full_event.clear()  # 如果磁盘队列为空, 队列job就可以继续添加任务了, 因此清除事件
                except TypeError:
                    logger.error(traceback.format_exc())
                finally:
                    lock.release()

class Threadpool():
    '''线程池'''

    q_upper_limit = 500000  # 任务队列长度上限
    def __init__(self, thread_num):
        self.thread_num = thread_num  # 线程池大小
        self.job = queue.Queue(self.q_upper_limit )  # 工作队列
        self.job2 = queuelib.FifoDiskQueue('.disk_queue')  # 磁盘工作队列
        self.threads = []  # 线程池
        self.current_finished = 0  # 当前层完成任务数初始化为0
        self.__init_threadpool(thread_num)  # 初始化线程池
        self.job_full_event = threading.Event()

    def __init_threadpool(self, thread_num):
        for i in range(thread_num):
            self.threads.append(Worker(self))

    def add_job(self, func, *args):
        '''将任务放入队列'''

        if self.job_full_event.is_set(): # 如果队列满事件已设置
            self.job2.push(pickle.dumps(args)) # 则将pickle过的任务参数push进磁盘队列job2
            logger.debug('push task into job2, job2 size: {}'.format(self.job2.info['size']))
        else:
            self.job.put((func, args)) # 如果队列满事件未设置, 则正常将任务加入队列job
            logger.debug('put task into job, job size: {}'.format(self.job.qsize()))
            if self.job.qsize() >= self.q_upper_limit: # 队列达到限制, 置位队列满事件
                logger.debug('!!!!! job full, tasks will be put into jobs2 !!!!!')
                self.job_full_event.set()

    def wait_completion(self):
        self.job.join()


def get_options():
    '''解析命令行选项'''
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', dest='url', action='store',
                      help='指定起始URL')
    parser.add_option('-d', '--depth', dest='depth', action='store',
                      type='int', default=2, help='指定爬取深度, 默认2层')
    parser.add_option('-f', '--logfile', dest='logfile', action='store',
                      default='spider.log', help='指定日志文件, 默认spider.log ')
    parser.add_option('-l', '--loglevel', dest='loglevel', action='store',
                      type='int', default=3, help='指定日志级别(1~5), 默认ERROR(3)级')
    parser.add_option('-k', '--key', dest='key',
                      action='store', help='指定关键词, 如未指定则保存所有网页')
    parser.add_option('-t', '--thread', dest='thread', action='store',
                      type='int', default=10, help='指定线程池大小, 默认10个线程')
    parser.add_option('--dbfile', dest='dbfile', action='store',
                      default='spider.db', help='指定数据库文件, 默认 spider.db')
    parser.add_option('--test', '--testself', dest='testself',
                      action='store_true', default=False, help='程序是否自测')
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
    form = logging.Formatter(
        "%(levelname)-8s %(asctime)s %(message)s")  # 日志显示格式
    handler = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=2048000, backupCount=3)  # 设置为旋转日志
    handler.setFormatter(form)
    logger.addHandler(handler)
    return logger


def main():
    db = Database(opt.dbfile)  # 数据库实例
    tp = Threadpool(opt.thread)  # 线程池实例
    c = Crawler(opt.url, opt.depth, db, opt.key, tp)  # 爬虫实例
    progress = Progress(c, tp)  # 进度实例
    tp.add_job(c.crawl, opt.url, 1)  # 将起始URL加入任务队列, 设置当前深度为1
    logger.info('tasks start!, destination url: {}, depth limitation: {}'.format(
        opt.url, opt.depth))
    try:
        tp.wait_completion()  # 等待所有任务完成
        progress.show_progress()
        shutil.rmtree('.disk_queue')
        c.total += c.current_total  # 任务完成后更新完成总数, 用于报告
        c.total_failed += c.current_failed
        c.total_finished += tp.current_finished
        logger.info('all tasks done')
    except KeyboardInterrupt:  # 按 Ctrl+C 退出
        print('please wait few seconds...')
        shutil.rmtree('.disk_queue')
        logger.info('task canceled by user')
        c.total += c.current_total
        c.total_failed += c.current_failed
        c.total_finished += tp.current_finished
        c.report()
        sys.exit('\n任务取消, 退出... \n')
    logger.info('exit normally')
    c.report()
    sys.exit('\n抓取完成')

if __name__ == '__main__':
    lock = threading.RLock()  # 线程锁
    opt = get_options()  # 获取命令行选项及参数
    if opt.testself:
        import doctest
        print(doctest.testmod())
        sys.exit('not implement\n')
    if not opt.url:
        sys.exit('Need to specify the URL, use "-h" see help\n')
    if not opt.url.startswith('http'):
        opt.url = 'http://' + opt.url
    logger = get_a_logger(opt.logfile, opt.loglevel)
    main()
