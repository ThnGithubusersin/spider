import requests
import queue
from threading import Thread, Lock
from lxml import etree
from urllib.error import URLError
import json
import time
import random


UserAgent_List = [
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36",
]

# 全局退出标志
exit_page_flag = False
exit_parse_flag = False

# 文件操作锁
lock = Lock()

# 爬虫线程
class SpiderThread(Thread):
    def __init__(self, id, url, page_queue, parse_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = id
        self.url = url
        self.page_queue = page_queue
        self.parse_queue = parse_queue

    def run(self):
        # 从page_queue取出要爬的页码,拼出url
        global exit_page_flag
        while True:
            # 当队列为空时 终止循环
            if self.page_queue.empty():
                break
            try:
                # 取数据
                page = self.page_queue.get(block=False)
                print('%d线程获取了%d页码' % (self.id, page))

                # 拼接url
                url = self.url % (page + 1)
                print(url)

                # 避免因为网络不好导致数据无法取出,尝试四次
                times = 4
                while times > 0:
                    try:
                        # 使用requests请求页面
                        response = requests.get(url, headers={'User-Agent': random.choice(UserAgent_List)})
                        # 把数据放入解析队列
                        self.parse_queue.put(response.text)
                        time.sleep(1)
                        # 事情做完才通知队列数据取出
                        self.page_queue.task_done()
                        break
                    except URLError:
                        print('网络错误!')
                    finally:
                        times -= 1
            except queue.Empty:
                pass


# 解析线程
class ParseThread(Thread):
    def __init__(self, id, fp, parse_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = id
        self.fp = fp
        self.parse_queue = parse_queue

    def run(self):
        # 拿到队列中的待解析数据
        global exit_parse_flag
        # 循环解析队列中的数据
        while True:
            # 当解析退出标志为True时,退出循环
            if exit_parse_flag:
                break
            try:
                data = self.parse_queue.get(block=False)
                # 解析数据,封装成一个函数
                self.parse(data, self.fp)
                print('%d线程解析数据成功' % self.id)
                # 通知队列数据取完
                self.parse_queue.task_done()
            except queue.Empty:
                pass

    # 爬取糗百
    def parse(self, data, fp):
        # 创建etree对象
        tree = etree.HTML(data)
        div_list = tree.xpath('//div[contains(@id,"qiushi_tag_")]')
        # 对div_list中的每一个div进行数据解析
        results = []
        for div in div_list:
            # 头像的url
            head_shot = div.xpath('.//img/@src')[0]
            # 作者名字
            name = div.xpath('.//h2')[0].text
            # 内容
            content = div.xpath('.//span')[0].text.strip('\n')
            item = {
                'head_shot': head_shot,
                'name': name,
                'content': content
            }
            results.append(item)
        #保存到文件中
        with lock:
            self.fp.write(json.dumps(results, ensure_ascii=False) + '\n')

# 两个队列
if __name__ == '__main__':
    print('主线程开始执行')
    page_queue = queue.Queue(10)
    # 只爬10页数据。往爬虫队列中写入10个数
    for page in range(10):
        page_queue.put(page)

    parse_queue = queue.Queue(10)
    url = 'https://www.qiushibaike.com/8hr/page/%d/'

    # 生成爬虫3个线程
    for i in range(3):
        SpiderThread(id=i, page_queue=page_queue, parse_queue=parse_queue, url=url).start()

    # 生成3个解析线程,并保存解析内容到本地文件
    fp = open('./qiubai.json', 'w+', encoding='utf-8')
    for i in range(3):
        ParseThread(id=i, fp=fp, parse_queue=parse_queue).start()

    # 队列锁,保证任务执行结束
    page_queue.join()
    parse_queue.join()

    # 设置关闭退出标志
    exit_parse_flag = True

    # 关闭文件
    fp.close()
    print('任务结束...')
