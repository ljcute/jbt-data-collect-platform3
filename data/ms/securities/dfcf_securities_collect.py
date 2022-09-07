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

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '东方财富证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 3:
                        # 东方财富证券融资融券标的证券采集
                        cls.rzrq_target_collect()
                    elif business_type == 2:
                        # 东方财富证券可充抵保证金证券采集
                        cls.guaranty_collect()
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except ProxyTimeOutEx as es:
                pass
            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    @classmethod
    def rzrq_target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集东方财富证券融资融券标的证券数据{actual_date}')
        driver = super().get_driver()
        # 融资融券标的证券
        url = 'https://www.xzsec.com/margin/ywxz/bdzqc.html'
        start_dt = datetime.datetime.now()
        driver.get(url)

        original_data_list = []

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
        cls.resolve_single_target_page(html_content, original_data_list)
        target_title = ['sec_code', 'sec_name', 'rz_rate', 'rq_rate', 'market']

        # 找到下一页 >按钮
        for_count = int(total_page) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            if current_page == int(total_page):
                temp_list = driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[6]/a')
                if temp_list:
                    driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[6]/a')[0].click()
            else:
                temp_list = driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[7]/a')
                if temp_list:
                    driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/div/ul/li[7]/a')[0].click()
            time.sleep(1)
            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东方财富标的券第{}页，共10条".format(current_page))
            cls.resolve_single_target_page(html_content, original_data_list)

        logger.info(f'总共采集条数{len(original_data_list)}')

        logger.info("采集东方财富证券融资融券标的证券数据结束")
        df_result = super().data_deal(original_data_list, target_title)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if df_result is not None:
            super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                exchange_mt_underlying_security,
                                data_source, start_dt, end_dt, used_time, url)
            logger.info(f'入库信息,共{int(len(original_data_list))}条')
        else:
            raise Exception(f'采集数据条数为0，需要重新采集')

        message = "dfcf_securities_collect"
        super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                  exchange_mt_underlying_security, data_source, message)

        logger.info("东方财富证券融资融券标的证券数据采集完成")

    @classmethod
    def guaranty_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集东方财富证券可充抵保证金证券数据{actual_date}')
        driver = super().get_driver()
        start_dt = datetime.datetime.now()
        # 可充抵保证金证券
        url = 'https://www.xzsec.com/margin/ywxz/bzjzqc.html'
        driver.get(url)
        original_data_list = []

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
        cls.resolve_single_target_page(html_content, original_data_list)
        target_title = ['sec_code', 'sec_name', 'rate', 'market', 'stock_group_name']

        # 找到下一页 >按钮
        for_count = int(total_page) + 1
        for current_page in range(2, for_count):
            driver.implicitly_wait(120)
            if current_page == int(total_page):
                temp_list = driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[6]/a')
                if temp_list:
                    driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[6]/a')[0].click()
            else:
                temp_list = driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[7]/a')
                if temp_list:
                    driver.find_elements(By.XPATH, '/html/body/div[3]/div/div[2]/div/div[2]/div/div[2]/div/ul/li[7]/a')[0].click()
            time.sleep(1)
            # 处理第[2, total_page]页html
            html_content = str(driver.page_source)
            logger.info("东方财富标的券第{}页，共10条".format(current_page))
            cls.resolve_single_target_page(html_content, original_data_list)

        logger.info(f'总共采集条数{len(original_data_list)}')

        logger.info("采集东方财富证券可充抵保证金担保券数据结束")
        df_result = super().data_deal(original_data_list, target_title)
        end_dt = datetime.datetime.now()
        used_time = (end_dt - start_dt).seconds
        if df_result is not None:
            super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                exchange_mt_guaranty_security,
                                data_source, start_dt, end_dt, used_time, url)
            logger.info(f'入库信息,共{int(len(original_data_list))}条')
        else:
            raise Exception(f'采集数据条数为0，需要重新采集')

        message = "dfcf_securities_collect"
        super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                  exchange_mt_guaranty_security, data_source, message)

        logger.info("东兴证券可充抵保证金担保券数据采集完成")

    @classmethod
    def resolve_single_target_page(cls, html_content, original_data_list):
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
            print(temp_list)
            original_data_list.append(temp_list)


if __name__ == '__main__':
    collector = CollectHandler()
    collector.collect_data(eval(sys.argv[1]))

    # collector.collect_data(2)
