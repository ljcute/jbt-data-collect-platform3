#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/3/3 16:49
# @Site    : 
# @File    : auto_trigger_collect.py
# @Software: PyCharm
import os
import sys

from collect_monitoring import monitoring
from data.ms.basehandler import logger


def auto_trigger_collect():
    logger.info(f'开始自动触发告警采集任务重新采集数据------')
    df = monitoring()
    df = df.loc[df['告警状态'] == '告警'].copy()
    df = df[['机构代码', 'type']]
    df = df['机构代码'].apply(lambda x: str(x).lower()) + ':' + df['type']
    securities_code = df.tolist()
    logger.info(f'securities_code:{securities_code}')
    for i in securities_code:
        temp_list = i.split(':')
        collector_name = temp_list[0] + '_' + 'securities_collect.py'
        collector_type = temp_list[1]
        os.system(
            f"cd /data/programs/python/jbt-data-collect-platform./securities.sh {collector_name} {collector_type}")
    logger.info(f'自动触发告警采集任务重新采集数据结束------')


if __name__ == '__main__':
    auto_trigger_collect()