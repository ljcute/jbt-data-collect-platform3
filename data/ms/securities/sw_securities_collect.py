#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 15:16
# 申万宏源

import os
import sys
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)


from utils.exceptions_utils import ProxyTimeOutEx
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder
import json
from selenium.webdriver.common.by import By
import time
import datetime
from bs4 import BeautifulSoup

from utils.logs_utils import logger

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '申万宏源'
url_ = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=2'
_url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=1'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 5:
            logger.info(f'重试第{max_retry}次')
            if business_type:
                if business_type == 3:
                    try:
                        # 申万宏源证券标的证券采集
                        cls.target_collect(max_retry)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}标的证券采集任务异常，请求url为：{url_}，具体异常信息为：{traceback.format_exc()}')
                elif business_type == 2:
                    try:
                        # 申万宏源证券可充抵保证金证券采集
                        cls.guaranty_collect(max_retry)
                        break
                    except ProxyTimeOutEx as es:
                        pass
                    except Exception as e:
                        logger.error(f'{data_source}可充抵保证金证券采集任务异常，请求url为：{_url}，具体异常信息为：{traceback.format_exc()}')

            max_retry += 1

    @classmethod
    def target_collect(cls, max_retry):
        actual_date = datetime.date.today()
        logger.info(f'开始采集申万宏源标的券数据{actual_date}')
        # 标的券
        start_dt = datetime.datetime.now()
        url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=2'

        try:
            driver = super().get_driver()
            driver.get(url)

            original_data_list = []
            # 找到券商官网写的总条数
            span_element = driver.find_elements(By.XPATH,
                                                "//*[@id='root']/section/div[2]/div/div[3]/div/div[1]/form/div[1]/div/div/div/span")
            # span_element = driver.find_elements(By.XPATH, "//*[@id='root']/section/div[2]/div/div[3]/div/div[1]/form/div[1]/div/div/div/span")
            sc_total = int(span_element[0].text)
            print(sc_total)
            # 当前网页内容(第1页)
            html_content = str(driver.page_source)
            logger.info("申万标的券第{}页，共10条".format(1))
            cls.resolve_single_target_page(html_content, original_data_list)
            target_title = ['market', 'secu_code', 'secu_name', 'rz_rate', 'rq_rate']
            time.sleep(1)
            # 找到总页数
            total_page = 0
            li_elements = driver.find_elements(By.XPATH,
                                               "//*[@id='root']/section/div[2]/div/div[3]/div/div[2]/div/div/div/ul/li[8]/a")
            if len(li_elements) > 0:
                total_page = li_elements[len(li_elements) - 1].text
            print(total_page)

            # 找到下一页 >按钮
            elements = driver.find_elements(By.XPATH, "//button[@class='ant-pagination-item-link']")
            next_page_button_element = elements[1]

            for_count = int(total_page) + 1  # range不包括后者
            for current_page in range(2, for_count):
                driver.execute_script('arguments[0].click();', next_page_button_element)
                time.sleep(0.8)

                # 处理第[2, total_page]页html
                html_content = str(driver.page_source)
                logger.info("申万标的券第{}页，共10条".format(current_page))
                cls.resolve_single_target_page(html_content, original_data_list)

            logger.info(f'采集申万宏源证券标的证券数据结束,共{int(len(original_data_list))}条')
            df_result = super().data_deal(original_data_list, target_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None and int(len(original_data_list)) == sc_total:
                data_status = 1
                super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(original_data_list))}条')
            elif int(len(original_data_list)) != sc_total:
                data_status = 2
                super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(original_data_list))}条')

            message = "sw_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_underlying_security, data_source, message)

            logger.info("申万宏源证券融资融券标的证券数据采集完成")
        except Exception as e:
            if max_retry == 4:
                data_status = 2
                super().data_insert(0, str(e), actual_date, exchange_mt_underlying_security,
                                    data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    @classmethod
    def resolve_single_target_page(cls, html_content, original_data_list):
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

    @classmethod
    def guaranty_collect(cls, max_retry):
        actual_date = datetime.date.today()
        logger.info(f'开始采集申万宏源担保券券数据{actual_date}')
        # 担保券（可充抵保证金）
        start_dt = datetime.datetime.now()
        url = 'https://www.swhysc.com/swhysc/financial/marginTradingList?channel=00010017000300020001&listId=1'

        try:
            driver = super().get_driver()
            driver.get(url)
            time.sleep(1)
            driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
            time.sleep(1)
            # driver.implicitly_wait(120)
            all_data_list = []
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
            cls.resolve_single_guaranty_page(html_content, all_data_list)

            db_title = ['market', 'secu_code', 'secu_name', 'rate', 'secu_type', 'secu_class']
            # 找到总页数
            total_page = 0
            # li_elements = driver.find_elements(By.XPATH, "//li[contains(@class, 'ant-pagination-item-647')]")
            li_elements = driver.find_elements(By.XPATH,
                                               "//*[@id='root']/section/div[2]/div/div[3]/div/div[2]/div/div/div/ul/li[8]/a")
            if len(li_elements) > 0:
                total_page = li_elements[len(li_elements) - 1].text
            print(total_page)
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
                logger.info("申万宏源担保券第{}页，共10条".format(current_page))
                cls.resolve_single_guaranty_page(html_content, all_data_list)

            logger.info(f'采集申万宏源证券担保券数据结束,共{int(len(all_data_list))}条')
            df_result = super().data_deal(all_data_list, db_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None and int(len(all_data_list)) == sc_total:
                data_status = 1
                super().data_insert(int(len(all_data_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(all_data_list))}条')
            elif int(len(all_data_list)) != sc_total:
                data_status = 2
                super().data_insert(int(len(all_data_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url, data_status)
                logger.info(f'入库信息,共{int(len(all_data_list))}条')

            message = "sw_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("申万宏源证券融资融券担保券数据采集完成")
        except Exception as e:
            if max_retry == 4:
                data_status = 2
                super().data_insert(0, str(e), actual_date, exchange_mt_guaranty_security,
                                    data_source, start_dt, None, None, url, data_status)

            raise Exception(e)

    @classmethod
    def resolve_single_guaranty_page(cls, html_content, all_data_list):
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
    # collector.collect_data(3)
    collector.collect_data(eval(sys.argv[1]))
