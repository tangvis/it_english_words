# -*- coding: utf-8 -*-
"""
@version: 0.1
@author: linus
@Email: linshaofeng1992@gmail.com
@file: crawler.py
@time: 2019/3/18 16:04
"""
import requests
from bs4 import BeautifulSoup
import re


sess = requests.Session()
sess.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36'
                          ' (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
            'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            'Accept-Language': "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
            'Accept-Encoding': "gzip, deflate, br",
            'Connection': "keep-alive"
        }
resp = sess.get('https://www.shanbay.com/wordbook/104791/')
soup = BeautifulSoup(resp.text, 'lxml')
url_list = list()
base_url = 'https://www.shanbay.com'
for i in soup.find_all('a', text=re.compile("程序员必学电脑计算机专业英语词汇 ")):
    url_list.append(base_url + i['href'])

data = list()

for url in url_list:
    resp = sess.get(url)
    soup = BeautifulSoup(resp.text, 'lxml')
    print(url)
    table = soup.find('table', {'class': 'table table-bordered table-striped'})
    for row in table.find_all('tr'):
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
    word_num = int(soup.select('#wordlist-num-vocab')[0].text)
    page_num = int(word_num / 20 + 1)
    for i in range(2, page_num + 1):
        an_url = url + '?page=' + str(i)
        resp = sess.get(an_url)
        soup = BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'table table-bordered table-striped'})
        for row in table.find_all('tr'):
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])

temp = list()
dict_ = dict()
for i in data:
    if i not in temp and len(i) == 2:
        dict_[i[0]] = i[1]

for i, j in dict_.items():
    dict_[i] = j.replace('\n', ' ')

tag = 0
with open('计算机词汇.txt', 'w') as f:
    for i, j in dict_.items():
        txt = str(tag) + '. ' + i + ': ' + j
        f.write(str(txt) + '\n\n')
        tag += 1
