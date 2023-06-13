#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description : 发送MQ
@Project     : jbt-data-collect-platform
@Date        : 2022-11-24
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""

import json
import os
import sys
import traceback
from configparser import ConfigParser
from datetime import datetime, date, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

from utils.logs_utils import logger
from kafka.producer import KafkaProducer
from data.dao.data_deal import get_data_source_and_data_type


base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
kafkaList = cf.get('kafka', 'kafkaList')
topic = cf.get('kafka', 'topic')
use_proxy = cf.get('proxy-switch', 'use_proxy')
out_cycle = cf.get('cycles', 'out_cycle')
in_cycle = cf.get('cycles', 'in_cycle')
biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券", 4: "融资标的证券", 5: "融券标的证券", 7: "单一股票担保物比例信息" , 99: "融资融券可充抵保证金证券和融资融券标的证券"}


def std_dt(dt):
    if isinstance(dt, str):
        if len(dt) == 8:
            return datetime.strptime(dt, '%Y%m%d')
        if len(dt) == 10:
            if dt.find('-') > 0:
                return datetime.strptime(dt, '%Y-%m-%d')
            elif dt.find('/') > 0:
                return datetime.strptime(dt, '%Y/%m/%d')
        elif len(dt) == 19:
            if dt.find('-') > 0:
                return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            elif dt.find('/') > 0:
                return datetime.strptime(dt, '%Y/%m/%d %H:%M:%S')


def get_dt(dt):
    if dt is None or (isinstance(dt, str) and len(dt) == 0):
        return date.today()
    elif isinstance(dt, str) and len(dt) >= 8:
        return std_dt(dt)
    if not type(dt) in (datetime, date):
        msg = f'时间格式错误: {dt}'
        logger.error(msg)
        raise Exception(msg)
    return dt


def hands_send_internet_data_collected_mq(data_source, biz_type, start_dt=None, end_dt=None):
    try:
        logger.info(f'hands_send_internet_data_collected_mq api parm: data_source={data_source}, biz_type={biz_type_map.get(biz_type)}, start_dt={start_dt}, end_dt={end_dt}')
        _start_dt = get_dt(start_dt)
        if isinstance(_start_dt, datetime):
            _start_dt = _start_dt.date()
        if end_dt is None:
            _end_dt = _start_dt
        else:
            _end_dt = get_dt(end_dt)
            if isinstance(_end_dt, datetime):
                _end_dt = _end_dt.date()
        producer = KafkaProducer(bootstrap_servers=kafkaList,
                                 key_serializer=lambda k: json.dumps(k).encode(),
                                 value_serializer=lambda v: json.dumps(v).encode())
        logger.info(f'hands_send_internet_data_collected_mq real parm: data_source={data_source}, biz_type={biz_type_map.get(biz_type)}, start_dt={_start_dt}, end_dt={_end_dt}')
        for i in range((_end_dt - _start_dt).days + 1):
            biz_dt = str(_start_dt + timedelta(days=i)).replace('-', '')
            logger.info(f'{data_source}{biz_type_map.get(biz_type)}, biz_dt={biz_dt}---开始发送mq消息')
            msg = {
                "user_id": 1,
                "biz_dt": biz_dt,
                "data_type": biz_type,
                "data_source": data_source,
                "message": '手工发送互联网数据采集完毕MQ'
            }
            future = producer.send(topic, value=str(msg), key=msg['user_id'], partition=0)
            record_metadata = future.get(timeout=30)
            logger.info(f'data_source={data_source}, biz_type={biz_type_map.get(biz_type)}, biz_dt={biz_dt}---topic partition:{record_metadata.partition}')
            logger.info(f'data_source={data_source}, biz_type={biz_type_map.get(biz_type)}, biz_dt={biz_dt}---发送mq消息结束，消息内容:{msg}')
    except Exception as e:
        logger.info(f"手工发送互联网数据采集完毕MQ异常：data_source parm: data_source={data_source}, biz_type={biz_type_map.get(biz_type)}, start_dt={start_dt}, end_dt={end_dt}: {e} =》{str(traceback.format_exc())}")


if __name__ == '__main__':
    try:
        dst = get_data_source_and_data_type()
        dst['data_type_cn'] = dst['data_type'].apply(lambda x: biz_type_map.get(int(x)))
        logger.info(f"data_source_ref_data_type: \n{dst}")
        msg = f"执行成功"
        argv = sys.argv
        if len(argv) > 2:
            data_source = argv[1]
            biz_type = int(argv[2])
            start_dt = None
            if len(argv) > 3:
                start_dt = argv[3]
            end_dt = None
            if len(argv) > 4:
                end_dt = argv[4]
            hands_send_internet_data_collected_mq(data_source, biz_type, start_dt, end_dt)
        else:
            raise f"手工发送互联网数据采集完毕MQ-入参{sys.argv}异常!"
        logger.info(f"手工发送互联网数据采集完毕MQ! 参数：{sys.argv}")
    except Exception as err:
        logger.error(f"手工发送互联网数据采集完毕MQ异常：{err} =》{str(traceback.format_exc())}")
