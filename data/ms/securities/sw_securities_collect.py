#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 15:16
# 申万宏源
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import fire
import datetime
from bs4 import BeautifulSoup
import pandas as pd
from data.dao import sh_data_deal
from utils.logs_utils import logger

broker_id = 10015
exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券

data_source = 'sw_securities'


def target_collect():
    logger.info("broker_id={}开始采集标的券".format(broker_id))
    # 创建chrome参数对象
    option = webdriver.ChromeOptions()
    option.add_argument("--headless")
    option.binary_location = r'C:\Users\jbt\AppData\Local\Chromium\Application\Chromium.exe'
    driver = webdriver.Chrome(executable_path='./chromedriver.exe', chrome_options=option)
    try:
        # 标的券
        start_dt = datetime.datetime.now()
        driver.get('https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=2')

        original_data_list = []
        local_date = time.strftime('%Y-%m-%d', time.localtime())

        # 找到券商官网写的总条数
        span_element = driver.find_elements(By.XPATH,
                                            "//div[contains(@class, 'ant-form-item-control-input-content')]/span")
        sc_total = int(span_element[0].text)
        print(sc_total)

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("申万标的券第{}页".format(1))
        resolve_single_target_page(html_content, original_data_list)
        target_title = ['market', 'secu_code', 'secu_name', 'rz_rate', 'rq_rate']

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, "//li[contains(@class, 'ant-pagination-item')]")
        if len(li_elements) > 0:
            total_page = li_elements[len(li_elements) - 1].text

        # 找到下一页 >按钮
        elements = driver.find_elements(By.XPATH, "//button[@class='ant-pagination-item-link']")
        next_page_button_element = elements[1]

        for_count = int(total_page) + 1  # range不包括后者
        for current_page in range(2, for_count):
            driver.execute_script('arguments[0].click();', next_page_button_element)
            time.sleep(0.8)

            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("申万标的券第{}页".format(current_page))
            resolve_single_target_page(html_content, original_data_list)

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        original_data_df = pd.DataFrame(data=original_data_list, columns=target_title)
        print(original_data_df)
        if original_data_df is not None:
            if original_data_df.iloc[:, 0].size == sc_total:
                df_result = {
                    'columns': target_title,
                    'data': original_data_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), local_date
                                                   , exchange_mt_financing_underlying_security, data_source, start_dt,
                                                   end_dt, used_time)
            else:
                logger.info("采集数据总数有误，请检查！")

    except Exception as es:
        logger.error(es)


def resolve_single_target_page(html_content, original_data_list):
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


def guaranty_collect():
    logger.info("broker_id={}开始采集担保券".format(broker_id))
    # 创建chrome参数对象
    option = webdriver.ChromeOptions()
    # 实现无可视化界面得操作
    option.add_argument("--headless")
    option.add_argument('--disable-gpu')
    option.binary_location = r'C:\Users\jbt\AppData\Local\Chromium\Application\Chromium.exe'
    driver = webdriver.Chrome(executable_path='./chromedriver.exe', chrome_options=option)

    try:
        # 担保券（可充抵保证金）
        start_dt = datetime.datetime.now()
        driver.get('https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=1')
        time.sleep(1)
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
        time.sleep(1)
        driver.implicitly_wait(120)

        local_date = time.strftime('%Y-%m-%d', time.localtime())
        all_data_list = []
        # 找到券商官网写的总条数
        span_element = driver.find_elements(By.XPATH,
                                            "//div[contains(@class, 'ant-form-item-control-input-content')]/span")
        sc_total = int(span_element[0].text)

        # 当前网页内容(第1页)
        html_content = str(driver.page_source)
        logger.info("申万担保券第{}页".format(1))
        resolve_single_guaranty_page(html_content, all_data_list)

        db_title = ['market', 'secu_code', 'secu_name', 'rate', 'secu_type', 'secu_class']

        # 找到总页数
        total_page = 0
        # li_elements = driver.find_elements(By.XPATH, "//li[contains(@class, 'ant-pagination-item')]")
        li_elements = driver.find_elements(By.XPATH, "//li[contains(@class, 'ant-pagination-item-647')]")
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
            time.sleep(0.8)
            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("申万担保券第{}页".format(current_page))
            resolve_single_guaranty_page(html_content, all_data_list)

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        guaranty_df = pd.DataFrame(data=all_data_list, columns=db_title)
        print(guaranty_df)
        if guaranty_df is not None:
            if guaranty_df.iloc[:, 0].size == sc_total:
                df_result = {
                    'columns': db_title,
                    'data': guaranty_df.values.tolist()
                }
                sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), local_date
                                                   , exchange_mt_guaranty_security, data_source, start_dt,
                                                   end_dt, used_time)

    except Exception as es:
        logger.error(es)


def resolve_single_guaranty_page(html_content, all_data_list):
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
    # target_collect()
    # guaranty_collect()

    fire.Fire()

    # python3 sw_securities_collect.py - target_collect
    # python3 sw_securities_collect.py - guaranty_collect
