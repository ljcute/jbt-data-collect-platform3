#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/18 16:50
# @Site    : 
# @File    : dfcf_securities_collect.py
# @Software: PyCharm
# 东方财富证券

import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
from utils.logs_utils import logger
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '东方财富'

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.xzsec.com/margin/ywxz/bdzqc.html'
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'rz_rate', 'rq_rate', 'market']
        driver = self.get_driver()
        driver.get(self.url)
        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH,
                                           "/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/div/select/option[1]")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text
            total_page = total_page[4:]

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("东方财富标的券第{}页,共10条".format(1))
        self.resolve_single_target_page(html_content, self.data_list)

        # 找到下一页 >按钮
        for_count = int(total_page) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            if current_page == int(total_page):
                temp_list = driver.find_elements(By.XPATH,
                                                 '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[6]/a')
                if temp_list:
                    driver.find_elements(By.XPATH,
                                         '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[6]/a')[
                        0].click()
            else:
                temp_list = driver.find_elements(By.XPATH,
                                                 '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[7]/a')
                if temp_list:
                    driver.find_elements(By.XPATH,
                                         '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[7]/a')[
                        0].click()
            time.sleep(0.5)
            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东方财富标的券第{}页，共10条".format(current_page))
            self.resolve_single_target_page(html_content, self.data_list)
            self.collect_num = int(len(self.data_list))
            self.total_num = int(len(self.data_list))

    def guaranty_securities_collect(self):
        self.url = 'https://www.xzsec.com/margin/ywxz/bzjzqc.html'
        self.data_list = []
        self.title_list = ['sec_code', 'sec_name', 'rate', 'market', 'stock_group_name']
        driver = self.get_driver()
        driver.get(self.url)
        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH,
                                           "/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/select/option[1]")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text
            total_page = total_page[4:]

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("东方财富保证金券第{}页,共10条".format(1))
        self.resolve_single_target_page(html_content, self.data_list)

        # 找到下一页 >按钮
        for_count = int(total_page) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            if current_page == int(total_page):
                temp_list = driver.find_elements(By.XPATH,
                                                 '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[6]/a')
                if temp_list:
                    driver.find_elements(By.XPATH,
                                         '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[6]/a')[
                        0].click()
            else:
                temp_list = driver.find_elements(By.XPATH,
                                                 '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[7]/a')
                if temp_list:
                    driver.find_elements(By.XPATH,
                                         '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[7]/a')[
                        0].click()
            time.sleep(0.3)
            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东方财富标的券第{}页，共10条".format(current_page))
            self.resolve_single_target_page(html_content, self.data_list)
            self.collect_num = int(len(self.data_list))
            self.total_num = int(len(self.data_list))

    def resolve_single_target_page(self, html_content, data_list):
        soup = BeautifulSoup(html_content, "html.parser")
        # label_td_div_list = soup.select('tbody tr')
        label_td_div_list = soup.select(
            'body > div.container-bg-one.rzrq > div > div.container > div > div.col-md-9.rzrq-list > div > div.rzrq-con-box1 > table > tbody > tr')
        # del label_td_div_list[0]
        new_tr_list = []
        for i in label_td_div_list:
            new_tr_list.append(i.text.replace('\n', ','))

        for s in new_tr_list:
            temp_list = s[1:-1].split(',')
            data_list.append(temp_list)


if __name__ == '__main__':
    # CollectHandler().collect_data(eval(sys.argv[1]))
    # CollectHandler().collect_data(2)
    pass