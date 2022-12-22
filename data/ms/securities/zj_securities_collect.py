#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/12/20 15:02
# @Site    :
# @File    : zj_securities_collect.py
# @Software: PyCharm
import os
import sys
import time
import json
import pandas as pd
import warnings
import concurrent.futures


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler, get_headers, logger, argv_param_invoke
from selenium.webdriver.common.by import By
from constants import *
from bs4 import BeautifulSoup


zj_headers_1 = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Cookie": "PORTALSESSIONID=A0A5D341C395B17EBE5EA1F4DED1A4B9; Hm_lvt_e1cd47d981c4fadfbb1623b1ebed716c=1620807780; Hm_lpvt_e1cd47d981c4fadfbb1623b1ebed716c=1620809962; oam.Flash.RENDERMAP.TOKEN=-ywvy7w75l",
    "Host": "www.ciccs.com.cn",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
}

class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中金公司'
        self.init_date = None
        self.url = ''
        self._proxies = self.get_proxies()
        self.total_page = 0

    def rz_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('rz')

    def rq_underlying_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect('rq')

    def guaranty_securities_collect(self):
        self.init_date = self.search_date.strftime('%Y%m%d')
        self._securities_collect_special()

    def _securities_collect_special(self):
        url_list = ["STOCK", "FUND", "BOND"]
        result = []
        for url in url_list:
            df = self.do_collect(url)
            if df.empty:
                continue
            result.append(df)
            time.sleep(3)
        print(result)
        _df = pd.concat(result)
        self.data_text = _df.to_csv(index=False)

    def do_collect(self, data_type):
        url = 'http://www.ciccs.com.cn/stocktrade/collateralList.xhtml'
        zj_headers_gu_2 = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            # "Content-Length": "1003",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "Hm_lvt_e1cd47d981c4fadfbb1623b1ebed716c=1620807780; PORTALSESSIONID=AC67AE9690F4C733EA3E0B49BA05165E; Hm_lpvt_e1cd47d981c4fadfbb1623b1ebed716c=1620813422; oam.Flash.RENDERMAP.TOKEN=-ywvy7w2rw",
            "Host": "www.ciccs.com.cn",
            "Origin": "http://www.ciccs.com.cn",
            "Referer": "http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml?type=MARGIN",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        is_continue = True
        page = 1
        lfs_value = '1'
        jsv_value = None
        while is_continue:
            logger.info(f'中金data_type={data_type},current_page={page}')
            if page == 1:
                params = {'type': data_type}
                response = session.get(url=url, proxies=self.proxies, params=params, headers=zj_headers_1, timeout=30)
            else:
                form_data = {
                    'listForm:search_txt': (None, None),
                    'pageNumber': (None, None),
                    'listForm_SUBMIT': (None, lfs_value),
                    'javax.faces.ViewState': (None, jsv_value),
                    'listForm:scroller': (None, 'next'),
                    'listForm:_idcl': (None, 'listForm:scrollernext'),
                }
                if page == 3:
                    zj_headers_gu_2['Referer'] = 'http://www.ciccs.com.cn/stocktrade/collateralList.xhtml'
                response = session.post(url=url, proxies=self.proxies, data=form_data, headers=zj_headers_gu_2,
                                        timeout=6)

            if int(response.status_code) != 200:
                logger.warning(f'response = {response}, current_page = {page}')
                sleep_second = random.randint(3, 5)  # 随机sleep 3-5秒
                time.sleep(sleep_second)
                continue
            df = pd.read_html(response.text)[2]
            df = df.loc[1:]
            self.tmp_df = pd.concat([self.tmp_df, df])
            warnings.filterwarnings("ignore")
            soup = BeautifulSoup(response.text, 'html.parser')
            jsv = soup.find('input', id='javax.faces.ViewState')
            jsv_value = jsv['value']  # 请求加密串，相当于cookie
            lfs = soup.find('input', attrs={'name': 'listForm_SUBMIT'})
            lfs_value = lfs['value']  # 1，标志
            _listForm_pageCount = soup.find('input', id='_listForm_pageCount')

            if page == 1:
                total = int(_listForm_pageCount['value'])  # 总页数

            if total <= page:
                is_continue = False
            else:
                page = page + 1
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)
        self.total_num = self.collect_num
        logger.info(f'已采集中金公司{data_type}数据条数为：{int(self.collect_num)}')
        return self.tmp_df


    # def driver_get(self, url):
    #     try:
    #         driver = self.get_driver()
    #         driver.get(url)
    #         driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
    #         time.sleep(3)
    #         # 找到券商官网写的总页数
    #         span_element = driver.find_elements(By.CSS_SELECTOR,
    #                                             "#listForm\:scroller > tbody > tr > td:nth-child(3) > div")
    #         temp_page = span_element[0].text.split(' ')
    #         self.total_page = int(temp_page[len(temp_page) - 1])
    #         # 当前网页内容(第1页)
    #         html_content = str(driver.page_source)
    #         _df1 = pd.read_html(html_content)[2]
    #         _df1.sort_values(by=['证券代码', '证券名称'], ascending=[True, True])
    #         self.tmp_df = pd.concat([self.tmp_df, _df1])
    #         self.collect_num = self.tmp_df.index.size
    #         current_page = 1
    #         logger.info(f"中金公司数据采集第{current_page}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
    #         while current_page < self.total_page:
    #             # 找到下一页 >按钮
    #             driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
    #             driver.implicitly_wait(10)
    #             elements = driver.find_elements(By.CSS_SELECTOR, "#listForm\:scrollernext > span")
    #             driver.implicitly_wait(10)
    #             next_page_button_element = elements[0]
    #             if not next_page_button_element.is_selected():
    #                 driver.execute_script('arguments[0].click();', next_page_button_element)
    #                 driver.implicitly_wait(10)
    #                 html_content = str(driver.page_source)
    #                 _df2 = pd.read_html(html_content)[2]
    #                 _df2.sort_values(by=['证券代码', '证券名称'], ascending=[True, True])
    #                 # 读取内容与上一次完全相同，则休息一下再重新读取内容
    #                 counter = 0
    #                 while _df1.equals(_df2):
    #                     logger.info(f'中金公司数据采集速度过快，休息一下{counter}')
    #                     time.sleep(1)
    #                     html_content = str(driver.page_source)
    #                     _df2 = pd.read_html(html_content)[2]
    #                     _df2.sort_values(by=['证券代码', '证券名称'], ascending=[True, True])
    #                     if counter > 100:
    #                         driver.quit()
    #                         msg = f"中金公司数据采集速度过快,连续休息100次重试还是存在问题!"
    #                         logger.info(msg)
    #                         raise Exception(msg)
    #                     counter += 1
    #                 _df1 = _df2
    #                 self.tmp_df = pd.concat([self.tmp_df, _df1])
    #                 # 与上一次存在重复，否则需要整个重采
    #                 if self.tmp_df.duplicated(['证券代码', '证券名称']).sum() > 0:
    #                     dep_line = self.tmp_df[
    #                         self.tmp_df.duplicated(['证券代码', '证券名称'], keep='last')]  # 查看删除重复的行
    #                     dep_list = dep_line.values.tolist()
    #                     msg = f'中金公司数据采集的重复数据为：{dep_list},共{len(dep_list)}条'
    #                     logger.info(msg)
    #                     raise Exception(msg)
    #                 self.collect_num = self.tmp_df.index.size
    #                 logger.info(
    #                     f"中金公司数据采集券第{current_page + 1}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
    #             current_page += 1
    #         self.data_text = self.tmp_df.to_csv(index=False)
    #         if self.collect_num > (int(self.total_page) - 1) * 10:
    #             self.total_num = self.collect_num
    #         return self.tmp_df
    #     finally:
    #         driver.quit()

    def _securities_collect(self, biz_type):
        if biz_type == 'rz':
            self.url = "http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml?type=MARGIN"
        elif biz_type == 'rq':
            self.url = "http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml?type=SHORTING"

        try:
            driver = self.get_driver()
            driver.get(self.url)
            driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
            time.sleep(3)
            # 找到券商官网写的总页数
            span_element = driver.find_elements(By.CSS_SELECTOR,
                                                "#listForm\:scroller > tbody > tr > td:nth-child(3) > div")
            temp_page = span_element[0].text.split(' ')
            self.total_page = int(temp_page[len(temp_page) - 1])
            # 当前网页内容(第1页)
            html_content = str(driver.page_source)
            _df1 = pd.read_html(html_content)[2]
            _df1.sort_values(by=['证券代码', '证券名称'], ascending=[True, True])
            self.tmp_df = pd.concat([self.tmp_df, _df1])
            self.collect_num = self.tmp_df.index.size
            current_page = 1
            logger.info(f"中金公司数据采集第{current_page}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
            while current_page < self.total_page:
                # 找到下一页 >按钮
                driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
                driver.implicitly_wait(10)
                elements = driver.find_elements(By.CSS_SELECTOR, "#listForm\:scrollernext > span")
                driver.implicitly_wait(10)
                next_page_button_element = elements[0]
                if not next_page_button_element.is_selected():
                    driver.execute_script('arguments[0].click();', next_page_button_element)
                    driver.implicitly_wait(10)
                    html_content = str(driver.page_source)
                    _df2 = pd.read_html(html_content)[2]
                    _df2.sort_values(by=['证券代码', '证券名称'], ascending=[True, True])
                    # 读取内容与上一次完全相同，则休息一下再重新读取内容
                    counter = 0
                    while _df1.equals(_df2):
                        logger.info(f'中金公司数据采集速度过快，休息一下{counter}')
                        time.sleep(1)
                        html_content = str(driver.page_source)
                        _df2 = pd.read_html(html_content)[2]
                        _df2.sort_values(by=['证券代码', '证券名称'], ascending=[True, True])
                        if counter > 100:
                            driver.quit()
                            msg = f"中金公司数据采集速度过快,连续休息100次重试还是存在问题!"
                            logger.info(msg)
                            raise Exception(msg)
                        counter += 1
                    _df1 = _df2
                    self.tmp_df = pd.concat([self.tmp_df, _df1])
                    # 与上一次存在重复，否则需要整个重采
                    if self.tmp_df.duplicated(['证券代码', '证券名称']).sum() > 0:
                        dep_line = self.tmp_df[
                            self.tmp_df.duplicated(['证券代码', '证券名称'], keep='last')]  # 查看删除重复的行
                        dep_list = dep_line.values.tolist()
                        msg = f'中金公司数据采集的重复数据为：{dep_list},共{len(dep_list)}条'
                        logger.info(msg)
                        raise Exception(msg)
                    self.collect_num = self.tmp_df.index.size
                    logger.info(
                        f"中金公司数据采集券第{current_page + 1}/{self.total_page}页，记录数{self.collect_num}/{self.total_num}条")
                current_page += 1
            self.data_text = self.tmp_df.to_csv(index=False)
            if self.collect_num > (int(self.total_page) - 1) * 10:
                self.total_num = self.collect_num
        finally:
            driver.quit()


if __name__ == '__main__':
    argv_param_invoke(CollectHandler(), (2, 4, 5), sys.argv)
    # CollectHandler().collect_data(2)