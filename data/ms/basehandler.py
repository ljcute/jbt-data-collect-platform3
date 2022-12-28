#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/12 13:56
# @Site    :
# @File    : basehandler.py
import json
import os
import sys
import time
import threading
import traceback
import pandas as pd
from config import Config
from datetime import datetime, date
from configparser import ConfigParser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from data.dao import data_deal
from utils.proxy_utils import get_proxies
from utils.logs_utils import logger
from selenium import webdriver
from kafka.producer import KafkaProducer
from constants import *

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
kafkaList = cf.get('kafka', 'kafkaList')
topic = cf.get('kafka', 'topic')
use_proxy = cf.get('proxy-switch', 'use_proxy')
out_cycle = cf.get('cycles', 'out_cycle')
in_cycle = cf.get('cycles', 'in_cycle')


def random_double(mu=0.8999999999999999, sigma=0.1000000000000001):
    """
        访问深交所时需要随机数
        :param mu:
        :param sigma:
        :return:
    """
    random_value = random.normalvariate(mu, sigma)
    if random_value < 0:
        random_value = mu
    return random_value


def random_page_size(mu=28888, sigma=78888):
    """
    获取随机分页数
    :param mu:
    :param sigma:
    :return:
    """
    random_value = random.randint(mu, sigma)  # Return random integer in range [a, b], including both end points.
    return random_value


def get_timestamp():
    return int(time.time() * 1000)


def std_dt(dt):
    _dt = dt
    if isinstance(dt, str):
        if len(dt) == 8:
                _dt = datetime.strptime(dt, '%Y%m%d')
        if len(dt) == 10:
            if dt.find('-') > 0:
                _dt = datetime.strptime(dt, '%Y-%m-%d')
            elif dt.find('/') > 0:
                _dt = datetime.strptime(dt, '%Y/%m/%d')
        elif len(dt) == 19:
            if dt.find('-') > 0:
                _dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            elif dt.find('/') > 0:
                _dt = datetime.strptime(dt, '%Y/%m/%d %H:%M:%S')
    return _dt


# 数据采集基类
class BaseHandler(object):

    biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券"
        , 4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}

    def __init__(self):
        # 数据来源：上交所、深交所、XXX券商等
        self.data_source = ''
        # 数据来源url
        self.url = ''
        # 数据类型是Excel时，Excel的路径
        self.excel_file_path = None
        # 采集的数据的业务类型：担保券、标的券、融资标的券、融券标的券等
        self.biz_type = 0
        # 实际采集的记录数
        self.collect_num = 0
        # 采集的数据里面记录的总记录数
        self.total_num = 0
        # 采集到的数据清单
        self.data_list = []
        self.tmp_df = pd.DataFrame()
        self.data_text = ''
        # 想采集数据的日期即采集维度里面的业务日期，和具体数据的业务日期无关：比如采集今天、历史某天(含节假日)
        self.search_date = date.today()
        # 数据采集执行开始时间
        self.start_dt = datetime.now()
        # 数据采集执行结束时间
        self.end_dt = self.start_dt
        self.mq_msg = None
        # 采集数据时，查询入参日期
        self.collect_num_check = True
        # 采集状态：默认2-失败状态，1成功，3采集成功-数据失败即未公布数据
        self.data_status = 2

    def collect_data(self, biz_type, search_date=None):
        if int(use_proxy) == 0:
            logger.info(f"{self.data_source}{self.biz_type_map.get(biz_type)}，此次数据采集不使用代理！")
        self.biz_type = biz_type
        if search_date is None or (isinstance(search_date, str) and len(search_date) == 0):
            search_date = date.today()
        elif isinstance(search_date, str) and len(search_date) >= 8:
            search_date = std_dt(search_date)
        if not type(search_date) in (datetime, date):
            msg = f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集任务执行失败！search_date 格式错误：{search_date}'
            logger.error(msg)
            raise Exception(msg)
        self.search_date = search_date
        logger.info(f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集任务开始执行{self.search_date}')
        max_retry = 0
        while max_retry < int(out_cycle):
            if max_retry > 0:
                logger.info(f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集重试第{max_retry}次')
            try:
                self.proxies = self.get_proxies()
                if biz_type == 0:
                    self.trading_amount_collect()
                elif biz_type == 1:
                    self.trading_items_collect()
                elif biz_type == 2:
                    self.guaranty_securities_collect()
                elif biz_type == 3:
                    self.rzrq_underlying_securities_collect()
                elif biz_type == 4:
                    self.rz_underlying_securities_collect()
                elif biz_type == 5:
                    self.rq_underlying_securities_collect()
                elif biz_type == 99:
                    self.guaranty_and_underlying_securities_collect()
                logger.info(f'{self.data_source}{self.biz_type_map.get(biz_type)}数据采集任务执行结束，已采集数据(条)：{self.collect_num}')
                self.end_dt = datetime.now()
                # 处理采集结果
                self.process_result()
                break
            except Exception as e:
                if max_retry == int(out_cycle) - 1:
                    logger.error(f'{self.data_source}{self.biz_type_map.get(biz_type)}采集任务异常，业务请求次数上限：{out_cycle}，已重试次数{max_retry}，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                    self.data_list.append(e)
                    self.process_result(True)
                time.sleep(30)
                self.tmp_df = pd.DataFrame()
            max_retry += 1

    # 交易所交易总量
    def trading_amount_collect(self):
        pass

    # 交易所交易明细
    def trading_items_collect(self):
        pass

    # 融资融券可充抵保证金证券
    def guaranty_securities_collect(self):
        pass

    # 融资融券标的证券
    def rzrq_underlying_securities_collect(self):
        pass

    # 融资标的证券
    def rz_underlying_securities_collect(self):
        pass

    # 融券标的证券
    def rq_underlying_securities_collect(self):
        pass

    # 融资融券可充抵保证金证券和融资融券标的证券
    def guaranty_and_underlying_securities_collect(self):
        pass

    def get_proxies(self):
        """
        use_proxy ==1 表示使用代理 ==0 表示不使用代理
        """
        if int(use_proxy) == 1:
            logger.info(f'{self.data_source}---开始获取代理ip')
            proxies = get_proxies()
            logger.info(f'获取代理ip结束,proxies:{proxies}')
            return proxies
        elif int(use_proxy) == 0:
            return None
        else:
            raise Exception("use_proxy参数有误，请检查")

    def get_response(self, url, request_type, headers=None, params=None, data=None,
                     allow_redirects=None, is_html=False):
        response = None
        max_retry = 0
        while max_retry < int(in_cycle):
            if max_retry > 0:
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}数据采集重试第{max_retry}次')
            try:
                if request_type == 0:
                    response = requests.get(url=url, params=params, proxies=self.proxies, headers=headers,
                                            allow_redirects=allow_redirects, timeout=30)
                elif request_type == 1:
                    response = requests.post(url=url, data=data, proxies=self.proxies, headers=headers, timeout=30)
                elif request_type == 2:
                    response = session.post(url=url, data=data, headers=headers, proxies=self.proxies, timeout=30)
                else:
                    logger.error(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，request_type参数有误')
                if response is None or (not is_html and response.status_code != 200):
                    raise Exception(f'{self.data_source}数据采集任务请求响应获取异常,已获取代理ip为:{self.proxies}，请求url为:{url},请求参数为:{params}')
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}数据采集: 总记录数(条){self.total_num}, 已采集数(条){self.collect_num}')
                if is_html:
                    return response
                else:
                    return response if response.status_code == 200 else None
            except Exception as e:
                self.proxies = self.get_proxies()
                headers = get_headers()
                if max_retry == int(in_cycle) - 1:
                    # logger.error(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，单次请求次数上限：{in_cycle}，已重试次数{max_retry}，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                    raise Exception(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url},Exception:{e},具体异常信息为:{traceback.format_exc()}')
                time.sleep(30)
            max_retry += 1

    def get_driver(self):
        max_retry = 0
        # if int(use_proxy) == 1:
        while max_retry < int(in_cycle):
            if max_retry > 0:
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}数据采集重试第{max_retry}次')
            try:
                # proxies = get_proxies()
                proxy = self.proxies
                if proxy is not None:
                    proxy = proxy['http']
                options = webdriver.FirefoxOptions()
                options.add_argument("--headless")
                options.add_argument("--proxy-server={}".format(proxy))
                options.binary_location = f"{Config().get_content('firefox').get('binary_location')}"
                driver = webdriver.Firefox(executable_path=f"{Config().get_content('firefox').get('executable_path')}", options=options)
                # driver = webdriver.Firefox(options=options)
                driver.implicitly_wait(10)
                return driver
            except Exception as e:
                logger.warn(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                self.proxies = self.get_proxies()
                if max_retry == int(in_cycle) - 1:
                    # logger.error(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url}，具体异常信息为:{traceback.format_exc()}')
                    raise Exception(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}采集任务异常，请求url为:{self.url},Exception:{e},具体异常信息为:{traceback.format_exc()}')
                time.sleep(30)
            max_retry += 1
        # elif int(use_proxy) == 0:
        #     raise Exception(f"{self.data_source}{self.biz_type_map.get(self.biz_type)}此次数据采集不使用代理,获取driver失败")
        # else:
        #     raise Exception(f"{self.data_source}{self.biz_type_map.get(self.biz_type)}use_proxy参数有误，请检查")

    def process_result(self, e_flag=None):
        used_time = (self.end_dt - self.start_dt).seconds
        if 0 < self.collect_num == self.total_num > 0:
            self.data_status = 1
            logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---开始进行数据入库')
            data_deal.insert_data_collect(self.collect_num, self.data_text, self.search_date, self.biz_type,
                                          self.data_source, self.start_dt,
                                          self.end_dt, used_time, self.url, self.data_status, self.excel_file_path)
            logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---数据入库结束,共{self.collect_num}条')
        else:
            if e_flag or not self.collect_num_check:
                if self.collect_num_check:
                    self.data_status = 2
                else:
                    self.data_status = 3
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---开始进行数据入库')
                data_deal.insert_data_collect(self.collect_num, self.data_text, self.search_date, self.biz_type,
                                              self.data_source, self.start_dt,
                                              self.end_dt, used_time, self.url, self.data_status, self.excel_file_path)
                logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---数据入库结束,共{self.collect_num}条')
            else:
                raise Exception(f'采集数据条数:{self.collect_num}与官网条数:{self.total_num}不一致，采集存在抖动，重新采集')

        self.kafka_mq_producer()

    def kafka_mq_producer(self):
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---开始发送mq消息')
        biz_dt = str(self.search_date).replace('-', '')[:8]
        producer = KafkaProducer(bootstrap_servers=kafkaList,
                                 key_serializer=lambda k: json.dumps(k).encode(),
                                 value_serializer=lambda v: json.dumps(v).encode())
        msg = {
            "user_id": 1,
            "biz_dt": biz_dt,
            "data_type": self.biz_type,
            "data_source": self.data_source,
            "message": self.mq_msg
        }
        future = producer.send(topic, value=str(msg), key=msg['user_id'], partition=0)
        record_metadata = future.get(timeout=30)
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---topic partition:{record_metadata.partition}')
        logger.info(f'{self.data_source}{self.biz_type_map.get(self.biz_type)}---发送mq消息结束，消息内容:{msg}')

    def refresh_proxies(self, _proxies):
        mutex = threading.Lock()
        mutex.acquire(60)  # 里面可以加blocking(等待的时间)或者不加，不加就会一直等待(堵塞)
        if _proxies == self._proxies:
            self._proxies = self.get_proxies()
        mutex.release()


def argv_param_invoke(handler, biz_types, argv):
    import datetime
    try:
        msg = f"执行成功"
        if len(argv) < 2 or int(argv[1]) not in biz_types:
            msg = f'执行成功失败：第一个入参[business_type]错误, 当前已支持业务类型[{biz_types}]！'
            logger.error(msg)
        elif 2 < len(argv) < 4:
            handler.collect_data(eval(argv[1]), argv[2])
        elif len(argv) == 2:
            handler.collect_data(eval(argv[1]))
        elif len(argv) == 4:
            a = argv[2].split('-')
            b = argv[3].split('-')
            begin = datetime.date(int(a[0]), int(a[1]), int(a[2]))
            end = datetime.date(int(b[0]), int(b[1]), int(b[2]))
            for i in range((end - begin).days + 1):
                day = begin + datetime.timedelta(days=i)
                day = str(day)
                logger.info(f'开始采集{day}的数据')
                handler.collect_data(eval(argv[1]), day)
        return msg
    except Exception as err:
        logger.error(f"互联网数据采集异常：{err} =》{str(traceback.format_exc())}")
