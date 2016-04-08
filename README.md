# [知道创宇爬虫题](http://blog.knownsec.com/2012/02/knownsec-recruitment/)

### 参数如下：

spider.py -u url -d deep -f logfile -l loglevel(1-5)  --testself --thread number --dbfile  filepath  --key=”HTML5”

 
### 参数说明：

 -u 指定爬虫开始地址

 -d 指定爬虫深度

 -t, --thread 指定线程池大小，多线程爬取页面，可选参数，默认10

 --dbfile 存放结果数据到指定的数据库（sqlite）文件中

 -k, --key 页面内的关键词，获取满足该关键词的网页，可选参数，默认为所有页面

 -l 日志记录文件记录详细程度，数字越大记录越详细，可选参数，默认spider.log

 --testself 程序自测，可选参数

### 程序依赖库
-python3-lxml
-python3-requests
-python3-bs4
