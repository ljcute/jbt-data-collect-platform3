#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/21 15:44
# 中金公司

import concurrent.futures
import os
import sys

import pandas


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(BASE_DIR)


from utils.exceptions_utils import ProxyTimeOutEx
from selenium.webdriver.common.by import By
from utils.proxy_utils import judge_proxy_is_fail
from bs4 import BeautifulSoup
from utils import remove_file
from data.ms.basehandler import BaseHandler
from utils.deal_date import ComplexEncoder, date_to_stamp
import json
import time
from constants import *
from utils.logs_utils import logger
import datetime

exchange_mt_guaranty_security = '2'  # 融资融券可充抵保证金证券
exchange_mt_underlying_security = '3'  # 融资融券标的证券
exchange_mt_financing_underlying_security = '4'  # 融资融券融资标的证券
exchange_mt_lending_underlying_security = '5'  # 融资融券融券标的证券
exchange_mt_guaranty_and_underlying_security = '99'  # 融资融券可充抵保证金证券和融资融券标的证券

data_source = '中金公司'

zj_headers_1 = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Cookie": "PORTALSESSIONID=A0A5D341C395B17EBE5EA1F4DED1A4B9; Hm_lvt_e1cd47d981c4fadfbb1623b1ebed716c=1620807780; Hm_lpvt_e1cd47d981c4fadfbb1623b1ebed716c=1620809962; oam.Flash.RENDERMAP.TOKEN=-ywvy7w75l",
    "Host": "www.ciccs.com.cn",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
}


class CollectHandler(BaseHandler):

    @classmethod
    def collect_data(cls, business_type):
        max_retry = 0
        while max_retry < 3:
            logger.info(f'重试第{max_retry}次')
            try:
                if business_type:
                    if business_type == 4:
                        # 中金公司融资标的证券采集
                        cls.rz_target_collect()
                    elif business_type == 5:
                        # 中金公司融券标的证券采集
                        cls.rq_target_collect()
                    elif business_type == 2:
                        # 中金公司可充抵保证金采集
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
    def rz_target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中金公司融资标的证券数据{actual_date}')
        url = 'http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml'
        zj_headers_rz_2 = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            # "Content-Length": "1003",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "Hm_lvt_e1cd47d981c4fadfbb1623b1ebed716c=1620807780; PORTALSESSIONID=AC67AE9690F4C733EA3E0B49BA05165E; Hm_lpvt_e1cd47d981c4fadfbb1623b1ebed716c=1620813422; oam.Flash.RENDERMAP.TOKEN=-ywvy7w2rw",
            "Host": "www.ciccs.com.cn",
            "Origin": "http://www.ciccs.com.cn",
            "Referer": "http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml?type=MARGIN",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        data_list = []
        data_title = ['stock_name', 'stock_code', 'is_flag', 'rate']
        is_continue = True
        page = 1
        lfs_value = '1'
        jsv_value = None

        try:
            start_dt = datetime.datetime.now()
            proxies = super().get_proxies()
            while is_continue:
                logger.info(f'中金公司融资,rz_current_page={page}')
                if page == 1:
                    params = {'type': 'MARGIN'}
                    response = session.get(url=url, proxies=proxies, params=params, headers=zj_headers_1, timeout=6)
                else:
                    form_data = {
                        'listForm:search_txt': (None, None),
                        'pageNumber': (None, None),
                        'listForm_SUBMIT': (None, lfs_value),
                        'javax.faces.ViewState': (None, jsv_value),
                        'listForm:scroller': (None, 'next'),
                        'listForm:_idcl': (None, 'listForm:scrollernext'),
                    }

                    if page == 3:
                        zj_headers_rz_2['Referer'] = 'http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml'
                    response = session.post(url=url, proxies=proxies, data=form_data, headers=zj_headers_rz_2,
                                            timeout=6)

                if int(response.status_code) != 200:
                    logger.warning(f'response = {response}, current_page = {page}')
                    sleep_second = random.randint(3, 5)  # 随机sleep 3-5秒
                    time.sleep(sleep_second)
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                jsv = soup.find('input', id='javax.faces.ViewState')
                jsv_value = jsv['value']  # 请求加密串，相当于cookie
                lfs = soup.find('input', attrs={'name': 'listForm_SUBMIT'})
                lfs_value = lfs['value']  # 1，标志
                _listForm_pageCount = soup.find('input', id='_listForm_pageCount')

                if page == 1:
                    total = int(_listForm_pageCount['value'])  # 总页数

                if total <= page:
                    is_continue = False
                else:
                    page = page + 1

                dom_td_list = soup.select('.contentTable td')

                for i in range(0, len(dom_td_list) - 1, 4):
                    stock_name = dom_td_list[i + 0].get_text()
                    stock_code = dom_td_list[i + 1].get_text()
                    is_flag = dom_td_list[i + 2].get_text()  # 是，否
                    rate = dom_td_list[i + 3].get_text()
                    data_list.append((stock_name, stock_code, is_flag, rate))
                    logger.info(f'已采集数据条数为：{int(len(data_list))}')

            logger.info(f'采集中金公司融资标的证券数据共{int(len(data_list))}条')
            df_result = super().data_deal(data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if int(len(data_list)) == int(len(df_result['data'])):
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_financing_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            else:
                raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total)}不一致，采集程序存在抖动，需要重新采集')

            message = "zj_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_financing_underlying_security, data_source, message)

            logger.info("中金公司融资标的证券数据采集完成")
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)

    @classmethod
    def rq_target_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中金公司融券标的证券数据{actual_date}')
        url = 'http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml'
        zj_headers_rq_2 = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            # "Content-Length": "1003",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "Hm_lvt_e1cd47d981c4fadfbb1623b1ebed716c=1620807780; PORTALSESSIONID=AC67AE9690F4C733EA3E0B49BA05165E; Hm_lpvt_e1cd47d981c4fadfbb1623b1ebed716c=1620813422; oam.Flash.RENDERMAP.TOKEN=-ywvy7w2rw",
            "Host": "www.ciccs.com.cn",
            "Origin": "http://www.ciccs.com.cn",
            "Referer": "http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml?type=SHORTING",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        data_list = []
        data_title = ['stock_name', 'stock_code', 'is_flag', 'rate']
        is_continue = True
        page = 1
        lfs_value = '1'
        jsv_value = None

        try:
            start_dt = datetime.datetime.now()
            proxies = super().get_proxies()
            while is_continue:
                logger.info(f'中金公司融券,rq_current_page={page}')
                if page == 1:
                    params = {'type': 'SHORTING'}
                    response = session.get(url=url, proxies=proxies, params=params, headers=zj_headers_1, timeout=6)
                else:
                    form_data = {
                        'listForm:search_txt': (None, None),
                        'pageNumber': (None, None),
                        'listForm_SUBMIT': (None, lfs_value),
                        'javax.faces.ViewState': (None, jsv_value),
                        'listForm:scroller': (None, 'next'),
                        'listForm:_idcl': (None, 'listForm:scrollernext'),
                    }
                    if page == 3:
                        zj_headers_rq_2['Referer'] = 'http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml'
                    response = session.post(url=url, proxies=proxies, data=form_data, headers=zj_headers_rq_2,
                                            timeout=6)

                if int(response.status_code) != 200:
                    logger.warning(f'response = {response}, current_page = {page}')
                    sleep_second = random.randint(3, 5)  # 随机sleep 3-5秒
                    time.sleep(sleep_second)
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                jsv = soup.find('input', id='javax.faces.ViewState')
                jsv_value = jsv['value']  # 请求加密串，相当于cookie
                lfs = soup.find('input', attrs={'name': 'listForm_SUBMIT'})
                lfs_value = lfs['value']  # 1，标志
                _listForm_pageCount = soup.find('input', id='_listForm_pageCount')

                if page == 1:
                    total = int(_listForm_pageCount['value'])  # 总页数

                if total <= page:
                    is_continue = False
                else:
                    page = page + 1

                dom_td_list = soup.select('.contentTable td')

                for i in range(0, len(dom_td_list) - 1, 4):
                    stock_name = dom_td_list[i + 0].get_text()
                    stock_code = dom_td_list[i + 1].get_text()
                    is_flag = dom_td_list[i + 2].get_text()  # 是，否
                    rate = dom_td_list[i + 3].get_text()
                    data_list.append((stock_name, stock_code, is_flag, rate))
                    logger.info(f'已采集数据条数为：{int(len(data_list))}')

            logger.info(f'采集中金公司融券标的证券数据共{int(len(data_list))}条')
            df_result = super().data_deal(data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if int(len(data_list)) == int(len(df_result['data'])):
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_lending_underlying_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            else:
                raise Exception(f'采集数据条数{int(len(data_list))}与官网数据条数{int(total)}不一致，采集程序存在抖动，需要重新采集')

            message = "zj_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_lending_underlying_security, data_source, message)

            logger.info("中金公司融券标的证券数据采集完成")
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)

    @classmethod
    def guaranty_collect(cls):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中金公司可充抵保证品数据{actual_date}')
        url = 'http://www.ciccs.com.cn/stocktrade/collateralList.xhtml'
        data_list = []
        data_title = ['stock_name', 'stock_code', 'rate', 'type']
        start_dt = datetime.datetime.now()
        try:
            logger.info(f'开始采集股票担保品数据')
            data_list_stock = []
            cls.do_guaranty_collect('STOCK', data_list_stock)
            logger.info(f'已采担保品-股票数据共{int(len(data_list_stock))}条')
            time.sleep(10)

            logger.info(f'开始采集基金担保品数据')
            data_list_fund = []
            cls.do_guaranty_collect('FUND', data_list_fund)
            logger.info(f'已采担保品-基金数据共{int(len(data_list_fund))}条')
            time.sleep(10)

            logger.info(f'开始采集债卷担保品数据')
            data_list_bond = []
            cls.do_guaranty_collect_get_bond('BOND', data_list_bond)
            logger.info(f'已采担保品-债券数据共{int(len(data_list_bond))}条')

            data_list.extend(data_list_stock)
            data_list.extend(data_list_fund)
            data_list.extend(data_list_bond)

            logger.info(f'已采担保品-股票，基金，债券数据共{int(len(data_list))}条')

            df_result = super().data_deal(data_list, data_title)
            end_dt = datetime.datetime.now()
            used_time = (end_dt - start_dt).seconds
            if data_list:
                super().data_insert(int(len(data_list)), df_result, actual_date,
                                    exchange_mt_guaranty_security,
                                    data_source, start_dt, end_dt, used_time, url)
                logger.info(f'入库信息,共{int(len(data_list))}条')
            else:
                raise Exception(f'采集数据条数条数为{int(len(data_list))}，需要重新采集')

            message = "zj_securities_collect"
            super().kafka_mq_producer(json.dumps(actual_date, cls=ComplexEncoder),
                                      exchange_mt_guaranty_security, data_source, message)

            logger.info("中金公司可充抵保证品数据采集完成")
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)
            raise Exception(f'{e}')

    @classmethod
    def do_guaranty_collect(cls, data_type, data_list):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中金公司{data_type}-担保品数据{actual_date}')
        url = 'http://www.ciccs.com.cn/stocktrade/collateralList.xhtml'
        zj_headers_gu_2 = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            # "Content-Length": "1003",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "Hm_lvt_e1cd47d981c4fadfbb1623b1ebed716c=1620807780; PORTALSESSIONID=AC67AE9690F4C733EA3E0B49BA05165E; Hm_lpvt_e1cd47d981c4fadfbb1623b1ebed716c=1620813422; oam.Flash.RENDERMAP.TOKEN=-ywvy7w2rw",
            "Host": "www.ciccs.com.cn",
            "Origin": "http://www.ciccs.com.cn",
            "Referer": "http://www.ciccs.com.cn/stocktrade/subjectMatterList.xhtml?type=MARGIN",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        is_continue = True
        page = 1

        lfs_value = '1'
        jsv_value = None
        try:
            proxies = super().get_proxies()
            while is_continue:
                logger.info(f'中金data_type={data_type},current_page={page}')
                if page == 1:
                    params = {'type': data_type}
                    response = session.get(url=url, proxies=proxies, params=params, headers=zj_headers_1, timeout=6)
                else:
                    form_data = {
                        'listForm:search_txt': (None, None),
                        'pageNumber': (None, None),
                        'listForm_SUBMIT': (None, lfs_value),
                        'javax.faces.ViewState': (None, jsv_value),
                        'listForm:scroller': (None, 'next'),
                        'listForm:_idcl': (None, 'listForm:scrollernext'),
                    }
                    if page == 3:
                        zj_headers_gu_2['Referer'] = 'http://www.ciccs.com.cn/stocktrade/collateralList.xhtml'
                    response = session.post(url=url, proxies=proxies, data=form_data, headers=zj_headers_gu_2,
                                            timeout=6)

                if int(response.status_code) != 200:
                    logger.warning(f'response = {response}, current_page = {page}')
                    sleep_second = random.randint(3, 5)  # 随机sleep 3-5秒
                    time.sleep(sleep_second)
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                jsv = soup.find('input', id='javax.faces.ViewState')
                jsv_value = jsv['value']  # 请求加密串，相当于cookie
                lfs = soup.find('input', attrs={'name': 'listForm_SUBMIT'})
                lfs_value = lfs['value']  # 1，标志
                _listForm_pageCount = soup.find('input', id='_listForm_pageCount')

                if page == 1:
                    total = int(_listForm_pageCount['value'])  # 总页数

                if total <= page:
                    is_continue = False
                else:
                    page = page + 1

                dom_td_list = soup.select('.contentTable td')

                for i in range(0, len(dom_td_list) - 1, 4):
                    stock_name = dom_td_list[i + 0].get_text()
                    stock_code = dom_td_list[i + 1].get_text()
                    rate = dom_td_list[i + 2].get_text()
                    type = dom_td_list[i + 3].get_text()
                    data_list.append((stock_name, stock_code, rate, type))
                    logger.info(f'已采集{data_type}数据条数为：{int(len(data_list))}')
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)

    @classmethod
    def do_guaranty_collect_get_bond(cls, data_type, data_list):
        actual_date = datetime.date.today()
        logger.info(f'开始采集中金公担保品-{data_type}数据{actual_date}')
        driver = super().get_driver()
        try:
            url = 'http://www.ciccs.com.cn/stocktrade/collateralList.xhtml?type=BOND'
            driver.get(url)

            # 找到总页数
            total_page = 0
            li_elements = driver.find_elements(By.XPATH, "//*[@id='listForm:scroller']/tbody/tr/td[3]/div")
            if len(li_elements) > 0:
                total_page = li_elements[len(li_elements) - 1].text
                total_page = total_page[-4:]

            # 当前网页内容(第1页)
            html_content = str(driver.page_source)
            logger.info(f'中金公司担保品-{data_type}第{1}页,共15条')
            cls.resolve_single_target_page(html_content, data_list)

            # 找到下一页 >按钮
            for_count = int(total_page.replace(',', '')) + 1
            for current_page in range(2, for_count):
                driver.implicitly_wait(120)
                elements = """return myfaces.oam.submitForm('listForm','listForm:scrollernext',null,[['listForm:scroller','next']]);;"""
                driver.execute_script(elements)
                time.sleep(0.6)
                driver.refresh()
                # 处理第[2, total_page]页html
                html_content = str(driver.page_source)
                logger.info(f'中金公司担保品-{data_type}的第{current_page}页，共15条')
                cls.resolve_single_target_page(html_content, data_list)

            logger.info(f'采集中金公司担保品-{data_type}相关数据结束,共{int(len(data_list))}条')
        except ProxyTimeOutEx as es:
            pass
        except Exception as e:
            logger.error(e)
            raise Exception(f'{e}')

    @classmethod
    def resolve_single_target_page(cls, html_content, original_data_list):
        soup = BeautifulSoup(html_content, "html.parser")
        label_td_div_list = soup.select('#listForm\:data\:tbody_element')
        if label_td_div_list:
            label_td_div_list = label_td_div_list
        else:
            label_td_div_list = soup.select('#listForm\:data > tbody')
        text = label_td_div_list[0].get_text()
        text_list = text.split('\n')
        text_list = text_list[1:-1]
        for i in text_list:
            if len(i) == 16:
                row_list = []
                row_list.append(i[0:6])
                row_list.append(i[6:11])
                row_list.append(i[11:14])
                row_list.append(i[14:])
            elif len(i) == 17:
                row_list = []
                row_list.append(i[0:6])
                row_list.append(i[6:12])
                row_list.append(i[12:15])
                row_list.append(i[15:])
            else:
                row_list = []
            original_data_list.append(row_list)


if __name__ == '__main__':
    collector = CollectHandler()
    # collector.collect_data(2)
    # collector.collect_data(eval(sys.argv[1]))
    if len(sys.argv) > 1:
        collector.collect_data(eval(sys.argv[1]))
    else:
        logger.error(f'business_type为必传参数')
