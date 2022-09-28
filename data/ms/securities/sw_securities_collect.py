#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 15:16
# 申万宏源

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
from utils.logs_utils import logger



class CollectHandler(BaseHandler):

    def __init__(self):
        super().__init__()
        self.mq_msg = os.path.basename(__file__).split('.')[0]
        self.data_source = '申万宏源'

    def rzrq_underlying_securities_collect(self):
        self.url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=2'
        driver = self.get_driver()
        driver.get(self.url)
        self.data_list = []

        # 找到券商官网写的总条数
        span_element = driver.find_elements(By.XPATH,
                                            "//*[@id='root']/section/div[2]/div/div[3]/div/div[1]/form/div[1]/div/div/div/span")
        # span_element = driver.find_elements(By.XPATH, "//*[@id='root']/section/div[2]/div/div[3]/div/div[1]/form/div[1]/div/div/div/span")
        sc_total = int(span_element[0].text)
        print(f'sc_total：{sc_total}')
        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("申万标的券第{}页，共10条".format(1))
        self.resolve_single_target_page(html_content, self.data_list)
        time.sleep(1)
        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH,
                                           "//*[@id='root']/section/div[2]/div/div[3]/div/div[2]/div/div/div/ul/li[8]/a")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text

        # 找到下一页 >按钮
        elements = driver.find_elements(By.XPATH, "//button[@class='ant-pagination-item-link']")
        next_page_button_element = elements[1]

        for_count = int(total_page) + 1  # range不包括后者
        for current_page in range(2, for_count):
            driver.execute_script('arguments[0].click();', next_page_button_element)
            time.sleep(1)

            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("申万标的券第{}页，共10条".format(current_page))
            self.resolve_single_target_page(html_content, self.data_list)
            self.collect_num = int(len(self.data_list))
        self.total_num = int(len(self.data_list))

    def resolve_single_target_page(self, html_content, original_data_list):
        soup = BeautifulSoup(html_content, "html.parser")
        label_td_div_list = soup.select(".ant-table-tbody .ant-table-row div")
        row_id = 0
        for k in label_td_div_list:
            if row_id % 5 == 0:
                # 开始,创建行对象
                row_list = []
                original_data_list.append(row_list)
            row_id += 1

            text = k.text
            if '%' in text:
                text = str(text).replace('%', '')
            row_list.append(text)

    def guaranty_securities_collect(self):
        self.url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=1'
        driver = self.get_driver()
        driver.get(self.url)
        self.data_list = []

        driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
        time.sleep(1)
        # driver.implicitly_wait(120)
        # 找到券商官网写的总条数
        # span_element = driver.find_elements(By.XPATH,
        #                                     "//div[contains(@class, 'ant-form-item-control-input-content')]/span")
        span_element = driver.find_elements(By.XPATH,
                                            "//*[@id='root']/section/div[2]/div/div[3]/div/div[1]/form/div[1]/div/div/div/span")
        sc_total = int(span_element[0].text)
        print(sc_total)
        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("申万宏源担保券第{}页，共10条".format(1))
        self.resolve_single_guaranty_page(html_content, self.data_list)

        # 找到总页数
        total_page = 0
        # li_elements = driver.find_elements(By.XPATH, "//li[contains(@class, 'ant-pagination-item-647')]")
        li_elements = driver.find_elements(By.XPATH,
                                           "//*[@id='root']/section/div[2]/div/div[3]/div/div[2]/div/div/div/ul/li[8]/a")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text
        # 找到下一页 >按钮
        elements = driver.find_elements(By.XPATH,
                                        "//button[@class='ant-pagination-item-link']/span[@class='anticon anticon-right']")
        next_page_button_element = elements[0]

        for_count = int(total_page) + 1  # range不包括后者
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            driver.execute_script('arguments[0].click();', next_page_button_element)
            time.sleep(1)
            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("申万宏源担保券第{}页，共10条".format(current_page))
            self.resolve_single_guaranty_page(html_content, self.data_list)
            self.collect_num = int(len(self.data_list))
        self.total_num = int(len(self.data_list))

    def resolve_single_guaranty_page(self, html_content, all_data_list):
        soup = BeautifulSoup(html_content, "html.parser")
        label_td_div_list = soup.select(".ant-table-tbody .ant-table-row div")
        row_id = 0
        for k in label_td_div_list:
            if row_id % 6 == 0:
                # 开始,创建行对象
                row_list = []
                all_data_list.append(row_list)
            row_id += 1

            text = k.text
            if row_id % 6 == 4:
                text = str(text).replace('%', '')
            row_list.append(text)


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(2)
    collector.collect_data(eval(sys.argv[1]))
