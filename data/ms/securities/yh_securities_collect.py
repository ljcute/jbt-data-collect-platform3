#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 13:34
# 中国银河证券
import os
import time
import json
from data.dao import sh_data_deal
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
from utils.logs_utils import logger
from utils.webdriver_utils import *
import datetime
import fire

os.environ['NUMEXPR_MAX_THREADS'] = "16"

# 定义常量
broker_id = 10011

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = 'yh_securities'


# 中国银河证券融资标的证券采集
def rz_target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(5)
    try:
        # 融资标的证券
        start_dt = datetime.datetime.now()
        driver.get('http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList')
        original_data_list = []
        original_data_title = ['sec_code', 'sec_name', 'margin_ratio']
        time.sleep(3)
        driver.find_elements(By.XPATH, '//a[text()="融资标的证券名单"]')[0].click()
        html_content = str(driver.page_source)
        resolve_page_content_rz(html_content, original_data_list)
        data_df = pd.DataFrame(data=original_data_list, columns=original_data_title)
        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': original_data_title,
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_financing_underlying_security, data_source, start_dt,
                                               end_dt, used_time)
            logger.info("broker_id={}完成数据入库".format(broker_id))

    except Exception as es:
        logger.error(es)
    finally:
        driver.quit()


def resolve_page_content_rz(html_content, original_data_list):
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


# 中国银河证券融券标的证券采集
def rq_target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(5)
    try:
        # 融券标的证券
        start_dt = datetime.datetime.now()
        driver.get('http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList')
        original_data_list = []
        original_data_title = ['sec_code', 'sec_name', 'margin_ratio']
        time.sleep(3)
        driver.find_elements(By.XPATH, '//a[text()="融券标的证券名单"]')[0].click()
        html_content = str(driver.page_source)
        resolve_page_content_rq(html_content, original_data_list)
        data_df = pd.DataFrame(data=original_data_list, columns=original_data_title)
        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': original_data_title,
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_lending_underlying_security, data_source, start_dt,
                                               end_dt, used_time)
            logger.info("broker_id={}完成数据入库".format(broker_id))

    except Exception as es:
        logger.error(es)
    finally:
        driver.quit()


def resolve_page_content_rq(html_content, original_data_list):
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


# 中国银河证券可充抵保证金证券名单采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(5)
    try:
        # 可充抵保证金证券
        start_dt = datetime.datetime.now()
        driver.get('http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList')
        original_data_list = []
        original_data_title = ['sec_code', 'sec_name', 'margin_ratio']
        time.sleep(3)
        driver.find_elements(By.XPATH, '//a[text()="可充抵保证金证券名单"]')[0].click()
        html_content = str(driver.page_source)
        resolve_page_content_bzj(html_content, original_data_list)
        data_df = pd.DataFrame(data=original_data_list, columns=original_data_title)
        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': original_data_title,
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_guaranty_security, data_source, start_dt,
                                               end_dt, used_time)
            logger.info("broker_id={}完成数据入库".format(broker_id))

    except Exception as es:
        logger.error(es)
    finally:
        driver.quit()


def resolve_page_content_bzj(html_content, original_data_list):
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
    # rz_target_collect()
    # rq_target_collect()
    # guaranty_collect()

    fire.Fire()

    # python3 yh_securities_collect.py - rz_target_collect
    # python3 yh_securities_collect.py - rq_target_collect
    # python3 yh_securities_collect.py - guaranty_collect