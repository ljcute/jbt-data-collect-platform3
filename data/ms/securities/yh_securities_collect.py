#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 13:34
# 中国银河证券

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler

import os
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

os.environ['NUMEXPR_MAX_THREADS'] = "16"


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '中国银河'
        self.url = 'http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList'

    def rz_underlying_securities_collect(self):
        driver = self.get_driver()
        driver.get(self.url)
        self.data_list = []
        time.sleep(3)
        driver.find_elements(By.XPATH, '//a[text()="融资标的证券名单"]')[0].click()
        html_content = str(driver.page_source)
        self.resolve_page_content_rz(html_content, self.data_list)
        self.collect_num = self.total_num = int(len(self.data_list))

    def resolve_page_content_rz(self, html_content, original_data_list):
        time.sleep(3)
        soup = BeautifulSoup(html_content, 'html.parser')
        collapseFour_content = soup.select('#table-bordered-finabcing')
        text_content = collapseFour_content[0].select('tr')
        for i in text_content:
            j = i.select('td')
            row_list = []
            for k in j:
                row_list.append(k.text)
            original_data_list.append(row_list)

    def rq_underlying_securities_collect(self):
        driver = self.get_driver()
        driver.get(self.url)
        self.data_list = []
        time.sleep(3)
        driver.find_elements(By.XPATH, '//a[text()="融券标的证券名单"]')[0].click()
        html_content = str(driver.page_source)
        self.resolve_page_content_rq(html_content, self.data_list)
        self.collect_num = self.total_num = int(len(self.data_list))

    def resolve_page_content_rq(self, html_content, original_data_list):
        time.sleep(3)
        soup = BeautifulSoup(html_content, 'html.parser')
        collapseFive_content = soup.select('#table-bordered-rong')
        text_content = collapseFive_content[0].select('tr')
        for i in text_content:
            j = i.select('td')
            row_list = []
            for k in j:
                row_list.append(k.text)
            original_data_list.append(row_list)

    def guaranty_securities_collect(self):
        driver = self.get_driver()
        driver.get(self.url)
        self.data_list = []
        time.sleep(3)
        driver.find_elements(By.XPATH, '//a[text()="可充抵保证金证券名单"]')[0].click()
        html_content = str(driver.page_source)
        self.resolve_page_content_bzj(html_content, self.data_list)
        self.collect_num = self.total_num = int(len(self.data_list))

    def resolve_page_content_bzj(self, html_content, original_data_list):
        time.sleep(3)
        soup = BeautifulSoup(html_content, 'html.parser')
        collapseSix_content = soup.select('#table-bordered-chong')
        text_content = collapseSix_content[0].select('tr')
        for i in text_content:
            j = i.select('td')
            row_list = []
            for k in j:
                row_list.append(k.text)
            original_data_list.append(row_list)


if __name__ == '__main__':
    collector = CollectHandler()
    collector.collect_data(eval(sys.argv[1]))
