#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 13:34
# 中国银河证券

import os
import sys

from utils.exceptions_utils import ProxyTimeOutEx

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)

from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder

import os
import time
import json
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from utils.logs_utils import logger
import datetime

os.environ['NUMEXPR_MAX_THREADS'] = "16"

# 定义常量
broker_id = 10011

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '中国银河证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 4:
                        # 银河证券融资标的证券采集
                        cls.rz_target_collect()
                    elif business_type == 5:
                        # 银河证券融券标的证券采集
                        cls.rq_target_collect()
                    elif business_type == 2:
                        # 银河证券可充抵保证金证券采集
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

    # 中国银河证券融资标的证券采集
    @classmethod
    def rz_target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中国银河证券融资标的证券相关数据{actual_date}')
        driver = super().get_driver()
        try:
            # 融资标的证券
            start_dt = datetime.datetime.now()
            url = 'http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList'
            driver.get(url)
            original_data_list = []
            original_data_title = ['sec_code', 'sec_name', 'margin_ratio']
            time.sleep(3)
            driver.find_elements(By.XPATH, '//a[text()="融资标的证券名单"]')[0].click()
            html_content = str(driver.page_source)
            cls.resolve_page_content_rz(html_content, original_data_list)

            logger.info(f'采集中国银河证券融资标的证券相关数据结束,共{int(len(original_data_list))}条')

            df_result = super().data_deal(original_data_list, original_data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None:
                super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(original_data_list))}条')
            else:
                raise Exception(f'采集数据条数为0，需要重新采集')

            message = "yh_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_financing_underlying_security, data_source, message)

            logger.info("中国银河证券融资标的证券相关数据采集完成")
        except ProxyTimeOutEx as e:
            pass
        except Exception as es:
            logger.error(es)
        finally:
            driver.quit()

    @classmethod
    def resolve_page_content_rz(cls, html_content, original_data_list):
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
    @classmethod
    def rq_target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中国银河证券融券标的证券相关数据{actual_date}')
        driver = super().get_driver()
        try:
            # 融券标的证券
            start_dt = datetime.datetime.now()
            url = 'http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList'
            driver.get(url)
            original_data_list = []
            original_data_title = ['sec_code', 'sec_name', 'margin_ratio']
            time.sleep(3)
            driver.find_elements(By.XPATH, '//a[text()="融券标的证券名单"]')[0].click()
            html_content = str(driver.page_source)
            cls.resolve_page_content_rq(html_content, original_data_list)

            logger.info(f'采集中国银河证券融券标的证券相关数据结束,共{int(len(original_data_list))}条')
            df_result = super().data_deal(original_data_list, original_data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds

            if df_result is not None:
                super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                    exchange_mt_lending_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(original_data_list))}条')
            else:
                raise Exception(f'采集数据条数为0，需要重新采集')

            message = "yh_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_lending_underlying_security, data_source, message)

            logger.info("中国银河证券融券标的证券相关数据完成")
        except ProxyTimeOutEx as e:
            pass
        except Exception as es:
            logger.error(es)
        finally:
            driver.quit()

    @classmethod
    def resolve_page_content_rq(cls, html_content, original_data_list):
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
    @classmethod
    def guaranty_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中国银河证券可充抵保证金证券相关数据{actual_date}')
        driver = super().get_driver()

        try:
            # 可充抵保证金证券
            start_dt = datetime.datetime.now()
            url = 'http://www.chinastock.com.cn/newsite/cgs-services/stockFinance/businessAnnc.html?type=marginList'
            driver.get(url)
            original_data_list = []
            original_data_title = ['sec_code', 'sec_name', 'margin_ratio']
            time.sleep(3)
            driver.find_elements(By.XPATH, '//a[text()="可充抵保证金证券名单"]')[0].click()
            html_content = str(driver.page_source)
            cls.resolve_page_content_bzj(html_content, original_data_list)

            logger.info(f'采集中国银河证券可充抵保证金证券相关数据结束,共{int(len(original_data_list))}条')
            df_result = super().data_deal(original_data_list, original_data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds

            if df_result is not None:
                super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(original_data_list))}条')
            else:
                raise Exception(f'采集数据条数为0，需要重新采集')

            message = "yh_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("中国银河证券可充抵保证金证券相关数据采集完成")
        except ProxyTimeOutEx as e:
            pass
        except Exception as es:
            logger.error(es)
        finally:
            driver.quit()

    @classmethod
    def resolve_page_content_bzj(cls, html_content, original_data_list):
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
    collector = CollectHandler()
    # collector.collect_data(3)
    collector.collect_data(eval(sys.argv[1]))
