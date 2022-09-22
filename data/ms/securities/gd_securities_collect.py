#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/06 15:16
# 光大证券

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
import time
from selenium.common import StaleElementReferenceException
from selenium.webdriver.common.by import By
from utils.logs_utils import logger


class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '光大证券'
        self.url = 'http://www.ebscn.com/ourBusiness/xyyw/rzrq/cyxx/'

    def rzrq_underlying_securities_collect(self):
        self.data_list = []
        self.title_list = ['market', 'sec_code', 'sec_name', 'financing_target', 'securities_mark', 'date']
        driver = self.get_driver()
        driver.get(self.url)

        # 第1页内容
        first_page_content = driver.find_elements(By.XPATH, '//*[@id="bdzq"]/div[2]/table/tbody/tr')[1:]
        logger.info("光大标的券第1页，共10条")
        self.resolve_every_page_bd(first_page_content, self.data_list)

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, '//*[@id="pageCount"]')
        if len(li_elements) > 0:
            total_page = ((li_elements[len(li_elements) - 2].text).split(' ')[1])[0:3]

        for_count = int(total_page) + 1  # range不包括后者
        for current_page in range(2, for_count):
            driver.find_elements(By.XPATH, '//*[@id="next_page"]')[0].click()
            time.sleep(0.5)
            try:
                this_page_content = driver.find_elements(By.XPATH, '//*[@id="bdzq"]/div[2]/table/tbody/tr')[1:]
            except StaleElementReferenceException as es:
                print(u'查找元素异常%s' % es)
                print(u'重新获取元素')
                this_page_content = driver.find_elements(By.XPATH, '//*[@id="bdzq"]/div[2]/table/tbody/tr')[1:]
                # 处理第[2, total_page]页html
                # html_content = str(driver.page_source)
            logger.info("光大标的券第{}页，共10条".format(current_page))
            self.resolve_every_page_bd(this_page_content, self.data_list)
            self.collect_num = int(len(self.data_list))
            self.total_num = int(len(self.data_list))

    def resolve_every_page_bd(self, this_page_content, data_list):
        if this_page_content:
            for i in this_page_content:
                row_list = []
                row_list.append((i.text).replace(' ', ','))
                temp_str = row_list[0]
                new_list = temp_str.split(',')
                if len(new_list) > 6:
                    another_list = new_list[2:(len(new_list) - 3)]
                    del new_list[2:(len(new_list) - 3)]
                    temp_str = ''.join(another_list)
                    new_list.insert(2, temp_str)
                data_list.append(new_list)

    def guaranty_securities_collect(self):
        self.data_list = []
        self.title_list = ['market', 'sec_code', 'sec_name', 'round_rate', 'date']
        driver = self.get_driver()
        driver.get(self.url)

        # 第1页内容
        first_page_content = driver.find_elements(By.XPATH, '//*[@id="bzj"]/div[2]/table/tbody/tr')[1:]
        logger.info("光大可充抵保证金券第1页，共10条")
        self.resolve_every_page_bzj(first_page_content, self.data_list)

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, '//*[@id="pageCount"]')
        if len(li_elements) > 0:
            total_page = ((li_elements[len(li_elements) - 1].text).split(' ')[1])[0:3]

        for_count = int(total_page) + 1  # range不包括后者
        for current_page in range(2, for_count):
            driver.find_elements(By.XPATH, '//*[@id="next_page"]')[1].click()
            time.sleep(0.5)
            try:
                this_page_content = driver.find_elements(By.XPATH, '//*[@id="bzj"]/div[2]/table/tbody/tr')[1:]
            except StaleElementReferenceException as es:
                print(u'查找元素异常%s' % es)
                print(u'重新获取元素')
                this_page_content = driver.find_elements(By.XPATH, '//*[@id="bzj"]/div[2]/table/tbody/tr')[1:]
                # 处理第[2, total_page]页html
                # html_content = str(driver.page_source)
            logger.info("光大可充抵保证金券第{}页，共10条".format(current_page))
            self.resolve_every_page_bzj(this_page_content, self.data_list)
            self.collect_num = int(len(self.data_list))
            self.total_num = int(len(self.data_list))


    def resolve_every_page_bzj(self, this_page_content, data_list):
        if this_page_content:
            for i in this_page_content:
                row_list = []
                row_list.append((i.text).replace(' ', ','))
                temp_str = row_list[0]
                new_list = temp_str.split(',')
                if len(new_list) > 5:
                    another_list = new_list[2:(len(new_list) - 2)]
                    del new_list[2:(len(new_list) - 2)]
                    temp_str = ''.join(another_list)
                    new_list.insert(2, temp_str)
                data_list.append(new_list)


if __name__ == '__main__':
    # CollectHandler().collect_data(eval(sys.argv[1]))
    # CollectHandler().collect_data(2)
    pass