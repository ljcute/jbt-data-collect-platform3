#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2023/02/23 15:16
import os
import sys
import time
import pandas as pd
from selenium.webdriver.common.by import By


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)
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
        temp_size = 10
        for i in range(1, self.total_page + 1):
            logger.info(f'第{i}页')
            param = {"page": i}
            response = self.get_response(self.url, 0, get_headers(), param)
            temp_df = pd.read_html(response.text)[0]
            self.tmp_df = pd.concat([self.tmp_df, temp_df])
            if i == 1:
                temp_size = temp_df.index.size

            while i < self.total_page and temp_size < temp_df.index.size:
                logger.info(f'条数不一致，需处理！')
                logger.info(f'第{i}页')
                param = {"page": i}
                response = self.get_response(self.url, 0, get_headers(), param)
                temp_df = pd.read_html(response.text)[0]
                self.tmp_df = pd.concat([self.tmp_df, temp_df])

        self.collect_num = self.tmp_df.index.size
        self.total_num = self.collect_num
        self.data_text = self.tmp_df.to_csv(index=False)


if __name__ == '__main__':
    # argv_param_invoke(CollectHandler(), (2, 3), sys.argv)
    CollectHandler().collect_data(2)