import os
from random import uniform
import urllib.error
from urllib import request
from fake_useragent import UserAgent
import re
from time import sleep, time
import sys
import csv
from tqdm import tqdm


class PiliSpider(object):
    def __init__(self, url: str):
        self.url = url
        self.url_cl = ''  # catelog, 示例：https://www.pilibook.net/5/5319_
        self.url_cpt_list = ['', 0]  # chapter，示例：https://www.pilibook.net/3/3438/547649.html
        self.len_cpt = 0
        self.ua = UserAgent()
        self.encode = 'gbk'
        self.timeout = 10
        self.retry = 3
        self.index = 0
        self.fo = None  # 输出文件
        self.fo_str = None
        self.csvfile = None
        self.flag_resume = False
        self.info_t = 0
        self.info_he = 0

    def get_html(self, url):
        def get_headers():
            return {"User-Agent": self.ua.random}
        for _ in range(self.retry):
            try:
                req = request.Request(url=url, headers=get_headers())
                res = request.urlopen(req, timeout=self.timeout)
                html = res.read().decode(self.encode, 'ignore')
                return html
            except urllib.error.HTTPError:
                self.info_he += 1
                sleep(uniform(1, 1.5))
                continue
            except (TimeoutError, urllib.error.URLError):
                self.info_t += 1
                continue

    def parse_html(self, html: str, re_bds: str) -> list:
        # '.'，支持匹配换行符
        pattern = re.compile(re_bds, flags=re.S)
        return pattern.findall(html)

    def save(self, content: str):
        # 保存单行数据
        self.fo.write(content)

    def back(self, state: list, flag_ind: bool):
        # 数据恢复
        self.len_cpt = int(state[0])
        self.fo = open(state[1], 'a+', encoding='utf-8')
        self.url_cpt_list = [state[2], int(state[3])]
        self.csvfile = open('progress.csv', 'a+', encoding='utf-8', newline='')
        if flag_ind:
            with open('ind.tmp', 'rt', encoding='utf-8') as ind:
                self.index = int(ind.read().strip())
                self.flag_resume = True
        print('进度已恢复')

    def info_getter(self,):
        print('获取书籍信息中...')
        html = self.get_html(self.url)
        re_bds = r'<div class="cataloginfo">\s+<h3>(.*?)</h3>\s+<div class="infotype">\s+<p>(作者名称：)<a href=".*?">(.*?)</a></p>\s+<p>(作品类型：.*?)</p>\s+<p>(更新时间：.*?)</p>\s+<p>(作品状态：.*?)</p>\s+</div>\s+</div>\s+<ul class="infolink">\s+<li class="margin_right"><b><a href=".*?">从头阅读</a></b></li>\s+<li><b><a href="(https://www.pilibook.net.*?)">章节目录</a></b></li>\s+<div class="clear"></div>\s+</ul>\s+<div class="intro">\s+<p>(.*?)</p>'
        info_tuple = self.parse_html(html, re_bds)[0]
        self.url_cl = info_tuple[6][:-1]+'_'
        info = '《{title}》\n{writer}\n{type}\n{update_time}\n{state}\n内容简介：\n”{intro}“\n'.format(title=info_tuple[0], writer=info_tuple[1]+info_tuple[2], type=info_tuple[3], update_time=info_tuple[4], state=info_tuple[5], intro=info_tuple[7])
        print(info)
        self.fo = open('{}.txt'.format(info_tuple[0]), 'a+', encoding='utf-8')
        self.fo_str = '{}.txt'.format(info_tuple[0])
        self.save(info+'\n')
        self.fo.flush()

    def catelog_getter(self):

        def cut_url(url) -> (str, int):
            for i in range(len(url)-1, -1, -1):
                if url[i] == '/':
                    return url[:i+1], int(url[i+1:])

        def single_page_saver():
            html = self.get_html(self.url_cl + str(i + 1) + '/')
            single_page = self.parse_html(html, re_bds_c)
            self.len_cpt += len(single_page)
            for title in single_page:
                self.save(title + '\n')
            sleep(uniform(1, 1.5))
            self.fo.flush()

        print('获取书籍目录中...')
        html = self.get_html(self.url_cl+'1/')
        re_bds_l = r'下一页</a><a href="/./(.*?)/">尾页</a></div>'
        # parse_html()返回：[str]
        len_ctl = int(self.parse_html(html, re_bds_l)[0][-1])
        re_bds_cpt = r"<ul class=\"chapters\">    <li><a href='(https://www.pilibook.net/.*?)'>"
        url_cpt = self.parse_html(html, re_bds_cpt)[0][:-5]
        self.url_cpt_list = cut_url(url_cpt)
        self.save('目录\n\n')
        re_bds_c = r"<li><a href='https://www\.pilibook\.net/.*?\.html'>(.*?)<span></span></a></li>"
        for i in tqdm(range(len_ctl), unit='chapter', colour='blue'):
            # single_page = [str,str,...]
            single_page_saver()
        print('目录获取完毕，长度：%d章\n' % self.len_cpt)
        self.save('\n')
        self.csvfile = open('progress.csv', 'a+', encoding='utf-8', newline='')
        state = csv.writer(self.csvfile, delimiter=' ', quotechar='|')
        state.writerow([str(self.len_cpt), self.fo_str, self.url_cpt_list[0], str(self.url_cpt_list[1])])
        self.csvfile.flush()

    def chapter_getter(self):
        # 正文下载
        print('开始下载正文...')
        if not self.flag_resume:
            self.save('正文\n\n')
        sleep(0.1)
        for i in tqdm(range(self.index, self.len_cpt), unit='chapter', colour='green'):
            html = self.get_html(self.url_cpt_list[0] + str(self.url_cpt_list[1] + i) + '.html')
            # 保存标题
            re_bds_t = r'\s+<h1 id="chaptertitle">(.*?)</h1>'
            self.save(self.parse_html(html, re_bds_t)[0]+'\n\n')
            # 保存正文
            re_bds_p = r'\s*<p>([^<].*?)</p>'
            for j in self.parse_html(html, re_bds_p):
                self.save('\t' + j + '\n')
            self.save('\n')
            self.fo.flush()
            # 记录当前下载进度
            self.index += 1
            # 保存index
            f_ind = open('ind.tmp', 'wt', encoding='utf-8')
            f_ind.write(str(self.index))
            f_ind.flush()
            # 反反爬
            sleep(uniform(1, 1.5))
        sleep(.5)
        print('\n小说下载结束\n')
        f_ind.close()

    def run(self):
        self.info_getter()
        self.catelog_getter()
        self.chapter_getter()
        os.remove('ind.tmp')
        self.csvfile.close()
        self.fo.close()


def intro():
    version = 0.2
    print("欢迎使用霹雳书坊小说下载器")
    print("作者：Deepwind")
    print('版本：v{}'.format(version))
    sleep(0.5)


def progress_resume():
    len_finished = 0  # 已完成下载的项目数
    with open('progress.csv', 'rt', newline='', encoding='utf-8') as csvfile:
        res = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for index, data in enumerate(res):
            if index == 0:
                len_url = int(data[0].strip())
            elif index == 1 and len_url != 1:
                url_list = list(data)
            else:
                if data == '':
                    break
                else:
                    len_finished += 1
    with open('progress.csv', 'rt', newline='', encoding='utf-8') as csvfile:
        res = csv.reader(csvfile, delimiter=' ', quotechar='|')
        flag_ind = False
        if 'ind.tmp' in os.listdir():
            flag_ind = True
        for index, data in enumerate(res):
            if len_url == index == 1 or (index == len_finished + 1 and len_url != 1):
                print('开始爬取小说，进度 {}/{}'.format(len_finished, len_url))
                state = list(data)
                spider = PiliSpider('')
                spider.back(state, flag_ind)
                spider.chapter_getter()
                spider.csvfile.close()
                os.remove('ind.tmp')
                break

    # 恢复结束，批量下载进入正常下载循环
    if len_url != 1:
        for index in range(len_finished, len_url):
            print('开始爬取小说，进度 {}/{}\n'.format(len_finished, len_url))
            process(url_list, index)
    os.remove('progress.csv')

def url_input():
    # TODO 解决两次回车
    print('请输入小说网址（支持小说首页，目录网址，可批量导入），注意需要多敲击回车一次：')
    url = []
    flag_input = True
    number = 1
    while flag_input:
        print(f'{number}.', end=' ')
        u = sys.stdin.readline().strip()
        re_main = re.compile(r'https://www\.pilibook\.net/book/\d+\.html')
        re_chapter = re.compile(r'https://www\.pilibook\.net/\d+/(\d+)/')
        if re_main.findall(u):
            url.append(u)
            number += 1
            continue
        elif num := re_chapter.findall(u):
            url.append('https://www.pilibook.net/book/' + num[0] + '.html')
            number += 1
            continue
        elif u == '':
            while True:
                flag_yn = input('输入结束？y/n \n').strip().lower()
                if flag_yn == 'y':
                    if len(url) == 0:
                        print('网址不可为空！')
                        break
                    else:
                        flag_input = False
                        break
                elif flag_yn == 'n':
                    print('请继续输入：')
                    break
                else:
                    print('y：Yes，n：No。请输入其中一个。\n')
        else:
            print('网址格式错误，请重新输入正确的网址！')
    return url


def process(url_list, index=0):
    start_time = time()
    spider = PiliSpider(url_list[index])
    spider.run()
    sleep(0.25)
    print('错误：{} 超时：{}'.format(spider.info_he, spider.info_t))
    print('爬取完毕，用时：%.2fs' % (time() - start_time))


def start():
    url_list = url_input()
    len_url = len(url_list)
    if len_url == 1:
        with open('progress.csv', 'wt', encoding='utf-8', newline='') as csvfile:
            url_save = csv.writer(csvfile, delimiter=' ', quotechar='|')
            url_save.writerow([len_url])
        process(url_list)
    elif len_url == 0:
        print('程序出错了 QAQ ,url列表长度为0！')
    else:
        with open('progress.csv', 'wt', encoding='utf-8', newline='') as csvfile:
            url_save = csv.writer(csvfile, delimiter=' ', quotechar='|')
            url_save.writerow([len_url])
            url_save.writerow(url_list)
        for i in range(len_url):
            print('开始爬取小说，进度 {}/{}'.format(i + 1, len_url))
            process(url_list, i)
    os.remove('progress.csv')


def test():
    test = PiliSpider('https://www.pilibook.net/book/3438.html')
    test.info_getter()
    test.catelog_getter()
    test.chapter_getter()


if __name__ == "__main__":
    intro()
    if 'progress.csv' in os.listdir():
        flag_yn = input('检测到程序上次运行非正常退出，请问是否要恢复进度? y/n \n')
        if flag_yn == 'y':
            progress_resume()
        elif flag_yn == 'n':
            start()
        else:
            print('y：Yes，n：No。请输入其中一个。\n')
    else:
        start()

