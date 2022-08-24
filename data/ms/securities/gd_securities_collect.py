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
from utils.deal_date import ComplexEncoder
import json
import datetime
import time
from selenium.common import StaleElementReferenceException
from selenium.webdriver.common.by import By
from utils.logs_utils import logger

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '光大证券'


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 3:
                        # 光大证券标的证券采集
                        cls.target_collect()
                    elif business_type == 2:
                        # 光大证券可充抵保证金证券采集
                        cls.guaranty_collect()
                    else:
                        logger.error(f'business_type{business_type}输入有误，请检查！')

                break
            except Exception as e:
                time.sleep(3)
                logger.error(e)

            max_retry += 1

    @classmethod
    def target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集光大证券融资融券标的证券数据{actual_date}')
        driver = super().get_driver()
        try:
            # 标的证券
            start_dt = datetime.datetime.now()
            url = 'http://www.ebscn.com/ourBusiness/xyyw/rzrq/cyxx/'
            driver.get(url)
            original_data_list = []
            original_data_title = ['market', 'sec_code', 'sec_name', 'financing_target', 'securities_mark', 'date']
            time.sleep(3)
            # 第1页内容
            first_page_content = driver.find_elements(By.XPATH, '//*[@id="bdzq"]/div[2]/table/tbody/tr')[1:]
            logger.info("光大标的券第1页，共10条")
            cls.resolve_every_page_bd(first_page_content, original_data_list)

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
                cls.resolve_every_page_bd(this_page_content, original_data_list)

            logger.info(f'采集光大证券标的证券数据结束,共{int(len(original_data_list))}条')
            df_result = super().data_deal(original_data_list, original_data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if df_result is not None:
                super().data_insert(int(len(original_data_list)), df_result, actual_date,
                                    exchange_mt_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(original_data_list))}条')
            else:
                raise Exception(f'采集数据条数为0，需要重新采集')

            # message = "gd_securities_collect"
            # super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
            #                           exchange_mt_underlying_security, data_source, message)

            logger.info("光大证券融资融券标的证券数据采集完成")
        except Exception as es:
            logger.error(es)
        finally:
            driver.quit()

    @classmethod
    def resolve_every_page_bd(cls, this_page_content, original_data_list):
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

    @classmethod
    def guaranty_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集光大证券可充抵保证金证券数据{actual_date}')
        driver = super().get_driver()
        try:
            # 可充抵保证金证券
            start_dt = datetime.datetime.now()
            url = 'http://www.ebscn.com/ourBusiness/xyyw/rzrq/cyxx/'
            driver.get(url)
            original_data_list = []
            original_data_title = ['market', 'sec_code', 'sec_name', 'round_rate', 'date']
            time.sleep(3)
            # 第1页内容
            first_page_content = driver.find_elements(By.XPATH, '//*[@id="bzj"]/div[2]/table/tbody/tr')[1:]
            logger.info("光大可充抵保证金券第1页，共10条")
            cls.resolve_every_page_bzj(first_page_content, original_data_list)

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
                cls.resolve_every_page_bzj(this_page_content, original_data_list)

            logger.info(f'采集光大证券可充抵保证金证券数据结束,共{int(len(original_data_list))}条')
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

            message = "gd_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("光大证券可充抵保证金证券数据采集完成")

        except Exception as es:
            logger.error(es)
        finally:
            driver.quit()



    @classmethod
    def resolve_every_page_bzj(cls, this_page_content, original_data_list):
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
    collector = CollectHandler()
    # collector.collect_data(3)
    collector.collect_data(eval(sys.argv[1]))
