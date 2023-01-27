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
import pymysql as sql


class PiliSpider(object):
    def __init__(self, url: str, db=False):
        self.url = url
        self.url_cl = ''  # catelog, 示例：https://www.pilibook.net/5/5319_
        self.url_cpt_list = ['', 0]  # chapter，示例：https://www.pilibook.net/3/3438/547649.html
        self.len_cpt = 0
        self.ua = UserAgent()
        self.encode = 'gbk'
        self.timeout = 8
        self.retry = 3
        self.index = 0
        self.fo = None  # 输出文件
        self.fo_str = None
        self.csvfile = None
        self.flag_resume = False
        self.info_t = 0
        self.info_he = 0
        # 如果db为False，返回None
        self.db = None
        self.cursor = None
        self.db_init(db)

    def db_init(self, db: bool):
        if db:
            self.db = sql.connect(host='localhost', user='root', password='Sqld_p1208', charset='utf8',
                                  database='novel')
            self.cursor = self.db.cursor(sql.cursors.DictCursor)
            cTable = """CREATE TABLE IF NOT EXISTS list{} (
                ID INT UNSIGNED NOT NULL AUTO_INCREMENT,
                TYPE CHAR(30) NOT NULL,
                URL VARCHAR(60) NOT NULL,
                TITLE CHAR(30) NOT NULL,
                AUTHOR CHAR(20) NOT NULL,
                INFO VARCHAR(1000),
                PRIMARY KEY (ID)
                )"""
            for i in range(9):
                self.cursor.execute(cTable.format(i))
            self.db.commit()

    def get_html(self, url) -> str:
        def get_headers():
            return {"User-Agent": self.ua.random}

        for _ in range(self.retry):
            try:
                req = request.Request(url=url, headers=get_headers())
                res = request.urlopen(req, timeout=self.timeout)
                html = res.read().decode(self.encode, 'ignore')
                if html:
                    return html
                else:
                    continue
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

    def save(self, content: str) -> None:
        # 保存单行数据
        self.fo.write(content)

    def back(self, state: list, flag_ind: bool) -> None:
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

    def rebuild(self, num: str):
        query1 = "drop table list{0}"
        query2 = """CREATE TABLE IF NOT EXISTS list{0} (
                        ID INT UNSIGNED NOT NULL AUTO_INCREMENT,
                        TYPE CHAR(30) NOT NULL,
                        URL VARCHAR(60) NOT NULL,
                        TITLE CHAR(30) NOT NULL,
                        AUTHOR CHAR(20) NOT NULL,
                        INFO VARCHAR(1000),
                        PRIMARY KEY (ID)
                        );"""
        if num.isdigit():
            self.cursor.execute(query1.format(int(num) - 1))
            self.db.commit()
            self.cursor.execute(query2.format(int(num) - 1))
            self.db.commit()
            print(f"榜单{num}已重载\n")
        else:
            for i in range(9):
                self.cursor.execute(query1.format(i))
                self.db.commit()
                self.cursor.execute(query2.format(i))
                self.db.commit()
            print("全部榜单已重载。\n")

    def top_getter(self, num: int) -> list:
        def better_print(a, b, c) -> None:
            def halftofull(name):
                s = []
                for x in name:
                    if ord(x) < 122:
                        s.append(chr(ord(x) + 65248))
                    else:
                        s.append(x)
                return ''.join(s)

            print('{type}{title}{author}'.format(type=a.ljust(name_len[0], chr(12288)),
                                                 title=halftofull(b).ljust(name_len[1], chr(12288)),
                                                 author=c.ljust(name_len[2], chr(12288))))

        def digit_check(numbers: str, maxnum=20) -> bool:
            nonlocal option
            num = numbers.split()
            flag = True
            if len(num) > 1:
                for n in num:
                    if not n.isdigit():
                        flag = False
                        break
                    else:
                        if int(n) > maxnum:
                            flag = False
                            break
                else:
                    flag = True
            else:
                return False
            if not flag:
                option = input('请输入正确的选项\n')
            return flag

        def single_novel(pos: int, cache: bool) -> str:
            # return Url:str or None
            # 此处的外部变量 url_list num page
            if not cache:
                print('数据加载中...')
            # 因为下面的info_getter不得不用类变量
            self.url = 'https://www.pilibook.net' + url_list[pos - 1]
            if not cache:
                print(self.info_getter(writeable=False))
            else:
                print(*show_info(num, page, pos=pos))
            while True:
                flag_yn = input('是否将该小说加入下载列表？y/n \n').strip().lower()
                if flag_yn == 'y':
                    return self.url
                elif flag_yn == 'n':
                    break
                else:
                    print('y：Yes，n：No。请输入其中一个。\n')

        def insert_sql(num: int, type: str, url: str, title: str, author: str):
            query = """INSERT INTO list{} (TYPE, URL, TITLE, AUTHOR)
            VALUES ('{}','{}','{}','{}')
            """.format(num, type, url, title, author)
            self.cursor.execute(query)
            self.db.commit()

        def info_update(num: int, url: str, info: str):
            query = """UPDATE list{} SET INFO = '{}' WHERE URL = '{}'""".format(num, info, url)
            self.cursor.execute(query)
            self.db.commit()

        def cache_check(num: int, page: int, item='*', pos=None) -> bool:
            if pos:
                self.cursor.execute(f"SELECT count({item}) FROM list{num} WHERE ID = {(page - 1) * 20 + pos}")
                return True if self.cursor.fetchall()[0][f'count({item})'] == 1 else False
            else:
                self.cursor.execute(
                    f"SELECT count({item}) FROM list{num} WHERE ID BETWEEN {(page - 1) * 20 + 1} and {page * 20}")
                return True if self.cursor.fetchall()[0][f'count({item})'] == 20 else False

        def show_info(num: int, page: int, item='INFO', pos=None) -> list:
            if not pos:
                self.cursor.execute(
                    f"SELECT {item} FROM list{num} WHERE ID BETWEEN ({(page - 1) * 20 + 1}) and {page * 20}")
            else:
                self.cursor.execute(f"SELECT {item} FROM list{num} WHERE ID = {pos + (page - 1) * 20}")
            items = self.cursor.fetchall()
            info = []
            for i in items:
                info.append(i[item])
            return info

        b_list = ['https://www.pilibook.net/top/dayvisit_', 'https://www.pilibook.net/top/weekvisit_',
                  'https://www.pilibook.net/top/monthvisit_', 'https://www.pilibook.net/top/allvisit_',
                  'https://www.pilibook.net/top/goodnum_', 'https://www.pilibook.net/top/size_',
                  'https://www.pilibook.net/top/postdate_', 'https://www.pilibook.net/top/lastupdate_',
                  'https://www.pilibook.net/full/']
        # num 为榜单序号 0~n
        page = 1
        url_list = []
        return_list = []
        while True:
            book_info = []  # 双层列表，[[type,url,title,],[]]
            cache_b = cache_check(num, page) if self.db else False
            if not cache_b:
                print('数据加载中...')
                top_html = self.get_html(f'{b_list[num]}{page}/')
                re_bds_t = r'<p class="p1">(.*?)</p><p class="p2">&nbsp;<a href="(.*?)" class="blue">(.*?)</a></p><p class="p3"><a href="/author/.*?">(.*?)</a></p></div>'
                book_info = self.parse_html(top_html, re_bds_t)
                name_len = [max(len(x[i]) for x in book_info) + 2 for i in range(4) if i != 1]
            else:
                bi = []
                for item in ['TYPE', 'URL', 'TITLE', 'AUTHOR']:
                    bi.append(show_info(num, page, item))
                for i in zip(*bi):
                    book_info.append(i)
                name_len = [max(len(x[i]) for x in book_info) + 2 for i in range(4) if i != 1]
            print(' ' * 4, end='')
            better_print('类型', '书名', '作者')
            for i in range(20):
                book = book_info[i]
                # 保存基本信息至数据库
                if self.db and (not cache_b):
                    insert_sql(num, *book)
                url_list.append(book[1])
                print(f'{i + 1:2}. ', end='')
                better_print(book[0], book[2], book[3])
            print(f"\n第{page}页")
            option = input("按F查看本页书籍详情，按A、D左右翻页，按E返回，输入序号可以查看对应书籍详情\n")
            info = []
            while True:
                if option.lower() == 'f':
                    cache_f = cache_check(num, page, 'INFO') if self.db else False
                    if not cache_f:
                        print('数据加载中...')
                        for u in tqdm(url_list):
                            self.url = 'https://www.pilibook.net' + u
                            inf = self.info_getter(writeable=False)
                            # 保存详情至数据库
                            if self.db:
                                info_update(num, u, inf)
                            info.append(inf)
                    else:
                        info = show_info(num, page)
                    for i, data in enumerate(info):
                        print(f"\n书籍序号：{i + 1}")
                        print(data, end='\n\n')
                    input("按任意键继续。\n")
                    break
                elif option.lower() == 'a':
                    if page == 1:
                        option = input("已经在第一页\n")
                        continue
                    else:
                        page -= 1
                        url_list = []
                    break
                elif option.lower() == 'd':
                    page += 1
                    url_list = []
                    break
                elif option.lower() == 'e':
                    if return_list:
                        print(f'本次添加了{len([url for url in return_list if url])}个网址。')
                        return return_list
                    else:
                        print('本次未添加任何网址。\n')
                        return [None]
                elif option.isdigit():
                    n = int(option)
                    cache_s = cache_check(num, page, item="INFO", pos=n) if self.db else False
                    return_list.append(single_novel(n, cache_s))
                    break
                elif digit_check(option):
                    nums = [int(x) for x in option.split()]
                    for n in nums:
                        print(f"书籍序号：{n}")
                        cache_s = cache_check(num, page, item="INFO", pos=n) if self.db else False
                        item = single_novel(n, cache_s)
                        return_list.append(item)
                    break
                else:
                    option = input('请输入正确的选项：\n')

    def info_getter(self, writeable=True):
        if writeable:
            print('获取书籍信息中...')
        html = self.get_html(self.url)
        re_bds = r'<div class="cataloginfo">\s+<h3>(.*?)</h3>\s+<div class="infotype">\s+<p>(作者名称：)<a href=".*?">(.*?)</a></p>\s+<p>(作品类型：.*?)</p>\s+<p>(更新时间：.*?)</p>\s+<p>(作品状态：.*?)</p>\s+</div>\s+</div>\s+<ul class="infolink">\s+<li class="margin_right"><b><a href=".*?">从头阅读</a></b></li>\s+<li><b><a href="(https://www.pilibook.net.*?)">章节目录</a></b></li>\s+<div class="clear"></div>\s+</ul>\s+<div class="intro">\s+<p>(.*?)</p>'
        info_tuple = self.parse_html(html, re_bds)[0]
        self.url_cl = info_tuple[6][:-1] + '_'
        info = '《{title}》\n{writer}\n{type}\n{update_time}\n{state}\n内容简介：\n”{intro}“\n'.format(title=info_tuple[0], writer=info_tuple[1] + info_tuple[2], type=info_tuple[3], update_time=info_tuple[4], state=info_tuple[5], intro=info_tuple[7])
        if writeable:
            print(info)
            self.fo = open('{}.txt'.format(info_tuple[0]), 'a+', encoding='utf-8')
            self.fo_str = '{}.txt'.format(info_tuple[0])
            self.save(info + '\n')
            self.fo.flush()
        else:
            return info

    def catelog_getter(self):

        def cut_url(url) -> (str, int):
            for i in range(len(url) - 1, -1, -1):
                if url[i] == '/':
                    return url[:i + 1], int(url[i + 1:])

        def single_page_saver():
            html = self.get_html(self.url_cl + str(i + 1) + '/')
            single_page = self.parse_html(html, re_bds_c)
            self.len_cpt += len(single_page)
            for title in single_page:
                self.save(title + '\n')
            sleep(uniform(1, 1.5))
            self.fo.flush()

        print('获取书籍目录中...')
        html = self.get_html(self.url_cl + '1/')
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
        # csv 进度写入
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
            # 保存标题 title
            re_bds_t = r'\s+<h1 id="chaptertitle">(.*?)</h1>'
            self.save(self.parse_html(html, re_bds_t)[0] + '\n\n')
            # 保存正文 paragraph
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
        sleep(0.1)
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
    version = 0.3
    print("欢迎使用霹雳书坊小说下载器")
    print("作者：Deepwind")
    print('版本：v{}\n'.format(version))
    sleep(0.1)


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
    lists = ['日点击榜', '周点击榜', '月点击榜', '总点击榜', '总收藏榜', '字数排行', '最新入库', '最新更新', '完本小说']
    url = []
    cache = False  # cache 默认关闭
    flag_input = True
    number = 1
    ps = None
    re_main = re.compile(r'https://www\.pilibook\.net/book/\d+\.html')
    re_chapter = re.compile(r'https://www\.pilibook\.net/\d+/(\d+)/')
    while flag_input:
        print('小说榜单：')
        for i in range(1, 10):
            print(f'{i}. {lists[i - 1]}\t', end='')
            if i % 3 == 0:
                print()
        print('\n请输入想要查看的榜单的序号或小说网址（支持小说首页，目录网址，可批量导入），注意需要多敲击回车一次：')
        print(f'{number}.', end=' ')
        u = sys.stdin.readline().strip()
        if len(u) == 1:
            if '1' <= u <= '9':
                ps = PiliSpider('', db=cache)
                u = ps.top_getter(int(u) - 1)
                for i in u:
                    if i:
                        url.append(i)
                        print(f'{number}. {i}\n')
                        number += 1
            elif u == '0':
                cache = True
                print('数据缓存已启用。\n')
                continue
        elif len(u) == 2 and u[0].lower() == 'r' and u[1] in ['a', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
            ps = PiliSpider('', db=True)
            ps.rebuild(u[1])
            del(ps)
        elif re_main.findall(u):
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
    # 输入完毕，关闭数据库
    if ps:
        ps.cursor.close()
        ps.db.close()
    return url


def process(url_list, index=0):
    start_time = time()
    spider = PiliSpider(url_list[index])
    spider.run()
    sleep(0.25)
    print('错误：{} 超时：{}'.format(spider.info_he, spider.info_t))
    print('爬取完毕，用时：%.2fs\n' % (time() - start_time))


def start():
    url_list = url_input()
    len_url = len(url_list)
    if len_url == 1:
        with open('progress.csv', 'wt', encoding='utf-8', newline='') as csvfile:
            url_save = csv.writer(csvfile, delimiter=' ', quotechar='|')
            url_save.writerow([len_url])
        process(url_list)
    else:
        with open('progress.csv', 'wt', encoding='utf-8', newline='') as csvfile:
            url_save = csv.writer(csvfile, delimiter=' ', quotechar='|')
            url_save.writerow([len_url])
            url_save.writerow(url_list)
        for i in range(len_url):
            print('开始爬取小说，进度 {}/{}'.format(i + 1, len_url))
            process(url_list, i)
    # csv文件回收
    os.remove('progress.csv')


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
