#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/3/3 16:49
# @Site    : 
# @File    : auto_trigger_collect.py
# @Software: PyCharm
import os
import subprocess
import sys
import traceback

from collect_monitoring import monitoring
from data.ms.basehandler import logger


# 当海豚调度中的采集任务再数次执行后仍然失败，则触发如下脚本程序自动重采失败采集任务
def auto_trigger_collect():
    logger.info(f'开始自动触发告警采集任务重新采集数据------')
    try:
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
            logger.info(f'本次重采{collector_name, collector_type}')
            logger.info(f'执行shell:（cd /data/jbt-data-collect-platform2/data/ms/securities; python3 {collector_name} {collector_type}）')
            os.system(f"cd /data/jbt-data-collect-platform2/data/ms/securities; python3 {collector_name} {collector_type}")
            # f = subprocess.Popen(f"cd /data/programs/python/jbt-data-collect-platform./securities.sh {
            # collector_name} {collector_type}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f'自动触发告警采集任务重新采集数据结束------')
    except Exception as e:
        logger.error(f'自动触发告警采集任务重新采集异常，具体异常信息为:{traceback.format_exc()}')


if __name__ == '__main__':
    auto_trigger_collect()