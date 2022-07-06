#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/07/06 15:16
# 光大证券
import json
import fire
import pandas as pd
from selenium import webdriver
import datetime
import time
from selenium.common import StaleElementReferenceException
from selenium.webdriver.common.by import By
from data.dao import sh_data_deal
from utils.logs_utils import logger

broker_id = 10015
exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '3.1'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '3.2'  # 融资融券融券标的证券

data_source = 'gd_securities'


# 光大证券标的证券采集
def target_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(10)
    try:
        # 标的证券
        start_dt = datetime.datetime.now()
        driver.get('http://www.ebscn.com/ourBusiness/xyyw/rzrq/cyxx/')
        original_data_list = []
        original_data_title = ['market', 'sec_code', 'sec_name', 'financing_target', 'securities_mark', 'date']
        time.sleep(3)
        # 第1页内容
        first_page_content = driver.find_elements(By.XPATH, '//*[@id="bdzq"]/div[2]/table/tbody/tr')[1:]
        logger.info("光大标的券第{}页".format(1))
        resolve_every_page_bd(first_page_content, original_data_list)

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, '//*[@id="pageCount"]')
        if len(li_elements) > 0:
            total_page = ((li_elements[len(li_elements) - 2].text).split(' ')[1])[0:3]
            print(total_page)

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
            logger.info("光大标的券第{}页".format(current_page))
            resolve_every_page_bd(this_page_content, original_data_list)

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data=original_data_list, columns=original_data_title)
        print(data_df)
        if data_df is not None:
            df_result = {
                'columns': original_data_title,
                'data': data_df.values.tolist()
            }
            sh_data_deal.insert_data_collect_1(json.dumps(df_result, ensure_ascii=False), query_date
                                               , exchange_mt_underlying_security, data_source, start_dt,
                                               end_dt, used_time)
            logger.info("broker_id={}完成数据入库".format(broker_id))

    except Exception as es:
        logger.error(es)
    finally:
        driver.quit()


def resolve_every_page_bd(this_page_content, original_data_list):
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
            original_data_list.append(new_list)


# 光大证券可充抵保证金证券采集
def guaranty_collect():
    query_date = time.strftime('%Y%m%d', time.localtime())
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(10)
    try:
        # 可充抵保证金证券
        start_dt = datetime.datetime.now()
        driver.get('http://www.ebscn.com/ourBusiness/xyyw/rzrq/cyxx/')
        original_data_list = []
        original_data_title = ['market', 'sec_code', 'sec_name', 'round_rate', 'date']
        time.sleep(3)
        # 第1页内容
        first_page_content = driver.find_elements(By.XPATH, '//*[@id="bzj"]/div[2]/table/tbody/tr')[1:]
        logger.info("光大可充抵保证金券第{}页".format(1))
        resolve_every_page_bzj(first_page_content, original_data_list)

        # 找到总页数
        total_page = 0
        li_elements = driver.find_elements(By.XPATH, '//*[@id="pageCount"]')
        if len(li_elements) > 0:
            total_page = ((li_elements[len(li_elements) - 1].text).split(' ')[1])[0:3]
            print(total_page)

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
            logger.info("光大可充抵保证金券第{}页".format(current_page))
            resolve_every_page_bzj(this_page_content, original_data_list)

        end_dt = datetime.datetime.now()
        # 计算采集数据所需时间used_time
        used_time = (end_dt - start_dt).seconds
        data_df = pd.DataFrame(data=original_data_list, columns=original_data_title)
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


def resolve_every_page_bzj(this_page_content, original_data_list):
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
            original_data_list.append(new_list)


if __name__ == '__main__':
    # target_collect()
    # guaranty_collect()
    fire.Fire()

    # python3 gd_securities_collect.py - target_collect
    # python3 gd_securities_collect.py - guaranty_collect
