#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/01 15:19
# 东兴证券

import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder

import datetime
import json
from utils.logs_utils import logger
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup

class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '东兴证券'

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.dxzq.net/main/rzrq/gsxx/rzrqdq/index.shtml?catalogId=1,10,60,144'
        self.data_list = []
        self.title_list = ['date', 'stock_code', 'stock_name', 'rz_rate', 'rq_rate']
        driver = self.get_driver()
        driver.get(self.url)
        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, "//span[contains(@class, 'all')]/em")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("东兴标的券第{}页,共10条".format(1))
        self.resolve_single_target_page(html_content, self.data_list)

        # 找到下一页 >按钮
        for_count = int(total_page) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            driver.execute_script("toPage({current_page})".format(current_page=current_page))
            time.sleep(1)

            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东兴标的券第{}页，共10条".format(current_page))
            self.resolve_single_target_page(html_content, self.data_list)
            self.collect_num = int(len(self.data_list))
            self.total_num = int(len(self.data_list))

    def resolve_single_target_page(self, html_content, data_list):
        soup = BeautifulSoup(html_content, "html.parser")
        # label_td_div_list = soup.select('tbody tr')
        label_td_div_list = soup.select('td:nth-child(-n+5)')
        # del label_td_div_list[0]
        row_id = 0
        for i in label_td_div_list:
            if row_id % 5 == 0:
                row_list = []
                data_list.append(row_list)
            row_id += 1

            text = i.text
            if '\n' in text:
                text = str(text).replace("\n", ",").strip()
                text = text[1:len(text) - 2]
            row_list.append(text)

    def guaranty_securities_collect(self):
        self.url = 'https://www.dxzq.net/main/rzrq/gsxx/kcdbzjzq/index.shtml?catalogId=1,10,60,145'
        self.data_list = []
        self.title_list = ['date', 'stock_code', 'stock_name', 'discount_rate']
        driver = self.get_driver()
        driver.get(self.url)
        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, "//span[contains(@class, 'all')]/em")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("东兴可充抵保证金券第{}页，共10条".format(1))
        self.resolve_single_target_page_ohter(html_content, self.data_list)
        # 找到下一页 >按钮
        for_count = int(total_page.replace(',', '')) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            driver.execute_script("toPage({current_page})".format(current_page=current_page))
            time.sleep(0.5)

            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东兴可充抵保证金券第{}页，共10条".format(current_page))
            self.resolve_single_target_page_ohter(html_content, self.data_list)
            self.collect_num = int(len(self.data_list))
            self.total_num = int(len(self.data_list))

    def resolve_single_target_page_ohter(self, html_content, data_list):
        soup = BeautifulSoup(html_content, "html.parser")
        label_td_div_list = soup.select('td:nth-child(-n+4)')
        row_id = 0
        for i in label_td_div_list:
            if row_id % 4 == 0:
                row_list = []
                data_list.append(row_list)
            row_id += 1

            text = i.text
            if '\n' in text:
                text = str(text).replace("\n", ",").strip()
                text = text[1:len(text) - 2]
            row_list.append(text)


if __name__ == '__main__':
    # CollectHandler().collect_data(eval(sys.argv[1]))
    # CollectHandler().collect_data(2)
    pass