#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/01 15:19
# 东兴证券
import datetime
import fire
import json
from data.dao import sh_data_deal
from utils.logs_utils import logger
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import pandas as pd

# 定义常量
broker_id = 10011

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'dx_securities'


# 东兴证券融资融券标的证券采集
def rzrq_target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集东兴证券融资融券标的证券券".format(broker_id))
    # 创建chrome参数对象
    option = webdriver.ChromeOptions()
    option.add_argument("--headless")
    option.binary_location = r'C:\Users\jbt\AppData\Local\Chromium\Application\Chromium.exe'
    driver = webdriver.Chrome(executable_path='./chromedriver.exe', chrome_options=option)
    try:
        # 融资融券标的证券
        start_dt = datetime.datetime.now()
        driver.get('https://www.dxzq.net/main/rzrq/gsxx/rzrqdq/index.shtml?catalogId=1,10,60,144')
        original_data_list = []

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, "//span[contains(@class, 'all')]/em")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text
        print(total_page)

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("东兴标的券第{}页".format(1))
        resolve_single_target_page(html_content, original_data_list)
        target_title = ['date', 'stock_code', 'stock_name', 'rz_rate', 'rq_rate']

        # 找到下一页 >按钮
        # elements = driver.find_elements(By.XPATH, "//button[@class='ant-pagination-item-link']")
        # next_page_button_element = elements[1]
        for_count = int(total_page) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            driver.execute_script("toPage({current_page})".format(current_page=current_page))
            time.sleep(1)

            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东兴标的券第{}页".format(current_page))
            resolve_single_target_page(html_content, original_data_list)

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        original_data_df = pd.DataFrame(data=original_data_list, columns=target_title)
        print(original_data_df)
        if original_data_df is not None:
            df_result = {
                'columns': target_title,
                'data': original_data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_underlying_security, data_source, start_dt,
                                               end_dt, used_time)

    except Exception as es:
        logger.error(es)


def resolve_single_target_page(html_content, original_data_list):
    soup = BeautifulSoup(html_content, "html.parser")
    # label_td_div_list = soup.select('tbody tr')
    label_td_div_list = soup.select('td:nth-child(-n+5)')
    # del label_td_div_list[0]
    row_id = 0
    for i in label_td_div_list:
        if row_id % 5 == 0:
            row_list = []
            original_data_list.append(row_list)
        row_id += 1

        text = i.text
        if '\n' in text:
            text = str(text).replace("\n", ",").strip()
            text = text[1:len(text) - 2]
        row_list.append(text)


# 东兴证券可充抵保证金证券采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    logger.info("broker_id={}开始采集东兴证券可充抵保证金担保券".format(broker_id))
    # 创建chrome参数对象
    option = webdriver.ChromeOptions()
    option.add_argument("--headless")
    option.binary_location = r'C:\Users\jbt\AppData\Local\Chromium\Application\Chromium.exe'
    driver = webdriver.Chrome(executable_path='./chromedriver.exe', chrome_options=option)
    try:
        start_dt = datetime.datetime.now()
        # 可充抵保证金证券
        driver.get('https://www.dxzq.net/main/rzrq/gsxx/kcdbzjzq/index.shtml?catalogId=1,10,60,145')
        original_data_list = []

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, "//span[contains(@class, 'all')]/em")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text
        print(total_page)

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("东兴可充抵保证金券第{}页".format(1))
        resolve_single_target_page_ohter(html_content, original_data_list)
        target_title = ['date', 'stock_code', 'stock_name', 'discount_rate']
        # 找到下一页 >按钮
        for_count = int(total_page.replace(',', '')) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            driver.execute_script("toPage({current_page})".format(current_page=current_page))
            time.sleep(0.5)

            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东兴可充抵保证金券第{}页".format(current_page))
            resolve_single_target_page_ohter(html_content, original_data_list)

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        original_data_df = pd.DataFrame(data=original_data_list, columns=target_title)
        print(original_data_df)
        if original_data_df is not None:
            df_result = {
                'columns': target_title,
                'data': original_data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_guaranty_security, data_source, start_dt,
                                               end_dt, used_time)

    except Exception as es:
        logger.error(es)


def resolve_single_target_page_ohter(html_content, original_data_list):
    soup = BeautifulSoup(html_content, "html.parser")
    label_td_div_list = soup.select('td:nth-child(-n+4)')
    row_id = 0
    for i in label_td_div_list:
        if row_id % 4 == 0:
            row_list = []
            original_data_list.append(row_list)
        row_id += 1

        text = i.text
        if '\n' in text:
            text = str(text).replace("\n", ",").strip()
            text = text[1:len(text) - 2]
        row_list.append(text)


if __name__ == '__main__':
    # rzrq_target_collect()
    # guaranty_collect()

    fire.Fire()

    # python3 dx_securities_collect.py - rzrq_target_collect
    # python3 dx_securities_collect.py - guaranty_collect
