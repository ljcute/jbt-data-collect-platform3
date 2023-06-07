#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2023/02/23 15:16
import os
import sys
import time
import pandas as pd
from selenium.webdriver.common.by import By
from io import StringIO


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
from data.dao.data_deal import get_failed_dfcf_collect
from data.ms.basehandler import BaseHandler, logger, argv_param_invoke
from constants import get_headers


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '东方财富'
        self.total_page = 0
        self.page_no = 1


    def rzrq_underlying_securities_collect(self):
        self.url = 'http://www.xzsec.com/margin/ywxz/bdzqc.html?code=&name=&market='
        self._securities_collect('bd')

    def guaranty_securities_collect(self):
        self.url = 'http://www.xzsec.com/margin/ywxz/bzjzqc.html'
        self._securities_collect('db')

    def _securities_collect(self, biz_type):
        # 获取总页数
        driver = self.get_driver()
        driver.get(self.url)
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
        time.sleep(3)
        if biz_type == 'db':
            li_elements = driver.find_elements(By.XPATH,"/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/select")
        elif biz_type == 'bd':
            li_elements = driver.find_elements(By.XPATH, "/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/div/select")
        else:
            raise Exception(f'业务类型(biz_type): {biz_type}错误')
        if len(li_elements) > 0:
            self.total_page = int(li_elements[-1].text[4:7])
        # 获取第一页数据条数，后面算总数
        response = self.get_response(self.url, 0, get_headers(), {"page": 1})
        first_df = pd.read_html(response.text)[0]
        # 获取最后一页数据条数，算出网页总数据
        param = {"page": self.total_page}
        response = self.get_response(self.url, 0, get_headers(), param)
        last_df = pd.read_html(response.text)[0]
        self.total_num = first_df.index.size * (self.total_page - 1) + last_df.index.size
        self.tmp_code_names = set([])
        if self._tmp_df.empty:
            prefix_ = get_failed_dfcf_collect(self.search_date, 2 if biz_type == 'db' else 3)
            if not prefix_.index.size == 0 and not len(prefix_['data_text'][0]) == 0:
                prefix_df = pd.read_csv(StringIO(prefix_['data_text'][0]), sep=",")
                # 回退3页，重采
                self.tmp_df = prefix_df.iloc[:(prefix_df.index.size - 30)]
        else:
            # 回退3页，重采
            self.tmp_df = self._tmp_df.iloc[:(self._tmp_df.index.size - 30)]
        # 已采记录数
        self.collect_num = self.tmp_df.index.size
        # 开采页码
        restart_page = int(self.collect_num / 10) + 1
        self.collect_pages(biz_type, restart_page)
        self.collect_num = self.tmp_df.index.size
        self.data_text = self.tmp_df.to_csv(index=False)

    def collect_pages(self, biz_type, restart_page):
        _pages = []
        if restart_page > 1:
            logger.info(f'断点重试，从{restart_page}页开始爬取！')
        for i in range(restart_page, self.total_page):
            param = {"page": i}
            logger.info(f'biz_type={biz_type}，第{i}页')
            response = self.get_response(self.url, 0, get_headers(), param)
            time.sleep(5)
            temp_df = pd.read_html(response.text)[0]
            code_names = set((temp_df['证券代码'].astype(str) + temp_df['证券简称']).to_list())
            if len(code_names.intersection(self.tmp_code_names)) > 0:
                logger.info(f"本次采集内容{(temp_df['证券代码'].astype(str) + temp_df['证券简称']).to_list()}")
                logger.info(f"本次重复内容{code_names.intersection(self.tmp_code_names)}")
                logger.info(f'第{i}页采集到重复数据，停止本次采集，下次进行断点重试！')
                break
            self.tmp_code_names = self.tmp_code_names.union(code_names)
            self.tmp_df = pd.concat([self.tmp_df, temp_df])
            self.collect_num = self.tmp_df.index.size


if __name__ == '__main__':
    # CollectHandler().collect_data(2)
    argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
