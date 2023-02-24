#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/21 10:28
# @Site    :
# @Software: PyCharm

import os
import sys
import pandas as pd
import pymysql
import pandas.io.sql as sqll
from configparser import ConfigParser
from datetime import datetime

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, 'config/config.ini')

cf = ConfigParser()
cf.read(full_path, encoding='utf-8')

host = cf.get('mysql', 'host')
port = cf.getint('mysql', 'port')
username = cf.get('mysql', 'username')
password = cf.get('mysql', 'password')
database = cf.get('mysql', 'schema')

biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券"
        , 4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}

def monitoring():
    currentDateAndTime = int(datetime.now().strftime("%H"))
    _df1 = None
    if currentDateAndTime < 12:
        # 不按时间过过滤的数据
        _df1 = get_data()
    elif currentDateAndTime > 12:
        # 按照时间过滤的数据
        dt = datetime.now().strftime("%Y-%m-%d")
        dt = dt + ' ' + '10:00:00'
        _df1 = get_data(dt)

    _df2 = get_normal_df()
    _df3 = _df1.loc[_df1['采集状态'] == '已采集'][
        ['机构ID', '机构代码', '机构名称', '数据日期', '采集时间', 'type', '业务类型', '采集状态', '已上线机构数', '已采集机构数']].copy()
    _df3['已上线机构数'] = _df3['已上线机构数'].astype(str)
    _df3['已采集机构数'] = _df3['已采集机构数'].astype(str)
    _df4 = get_security_df()
    _df4.rename(columns={'broker_id': '机构ID', 'broker_code': '机构代码', 'broker_name': '机构名称', 'valid': '上线状态', 'order_no': '排名'},
                inplace=True)
    _df4['上线状态'] = _df4['上线状态'].apply(lambda x: '已上线' if x == '1' else '未上线')
    _df2 = pd.merge(_df2, _df4[['机构ID', '上线状态', '排名']], how='left', on=['机构ID'])
    rs = pd.merge(_df2, _df3, how='left', on=['机构ID', '机构代码', '机构名称', 'type'])
    rs['采集状态'].fillna('未采集', inplace=True)
    rs['采集状态'].fillna('未采集', inplace=True)
    rs['业务类型'] = rs['type'].apply(lambda x: biz_type_map.get(int(x)))
    rs['告警状态'] = rs['上线状态'] + rs['采集状态']
    rs['告警状态'] = rs['告警状态'].apply(lambda x: '告警' if x == '已上线未采集' else '正常')
    rs.sort_values(by=['告警状态', '上线状态', '采集状态', '排名', '机构ID', '机构代码', '机构名称', 'type'], inplace=True)
    rs.fillna('-', inplace=True)
    rs.reset_index(inplace=True, drop=True)
    rs = rs[['数据日期', '已上线机构数', '已采集机构数', '机构ID', '机构代码', '机构名称', 'type', '业务类型', '上线状态', '排名', '采集状态', '采集时间', '告警状态']]
    rs['数据日期'] = _df1['数据日期'][0]
    rs['已上线机构数'] = _df1['已上线机构数'][0]
    rs['已采集机构数'] = _df1['已采集机构数'][0]
    return rs


def get_data(dt=None):
    conn = pymysql.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        passwd=password,
    )
    if dt is not None:
        dt_str = f" and (create_dt > '{dt}' or SUBSTR(data_source, 3, 3 )='交易所')"
    else:
        dt_str = ""

    sql = f"""
                    SELECT
            a.broker_id AS `机构ID`,
            a.broker_code AS `机构代码`,
            ifnull( b.data_source, a.broker_name ) AS `机构名称`,
            IFNULL( b.biz_dt, '-' ) AS `数据日期`,
            IFNULL( b.create_dt, '-' ) AS `采集时间`,
            b.data_type as type,
            (CASE WHEN b.data_type = 0 THEN '交易总量' WHEN b.data_type = 1 THEN '交易明细' WHEN b.data_type = 2 THEN '担保券' WHEN b.data_type = 3 THEN '标的券' WHEN b.data_type = 4 THEN '融资标的券' WHEN b.data_type = 5 THEN '融券标的券' WHEN b.data_type = '99' THEN '担保券及标的券' END) AS '业务类型',
            '已采集' AS `采集状态`,
            ( SELECT COUNT( DISTINCT broker_id ) FROM `db-internet-biz-data`.`t_security_broker` WHERE valid = 1 ) AS `已上线机构数`,
            (
            SELECT
                COUNT( DISTINCT data_source ) 
            FROM
                `db-internet-raw-data`.`t_ndc_data_collect_log`
            WHERE
                data_source NOT LIKE '%交易所' 
                AND biz_dt =(
                SELECT
                    MAX( biz_dt ) 
                FROM
                    t_ndc_data_collect_log 
                )) AS `已采集机构数` 
        FROM
            `db-internet-biz-data`.`t_security_broker` a
            LEFT JOIN (
            SELECT 
                biz_dt, max(create_dt) as create_dt,
                data_source,
                data_type,
                data_status
            FROM
                `db-internet-raw-data`.`t_ndc_data_collect_log`
            WHERE
                data_status = 1
                {dt_str}
                AND biz_dt =(
                SELECT
                    MAX( biz_dt ) 
                FROM
                `db-internet-raw-data`.`t_ndc_data_collect_log`)
                group by biz_dt, data_source, data_type, data_status
                ) b ON (
                a.broker_name = b.data_source 
            OR a.broker_name = SUBSTR( b.data_source, 3, 3 )) 
        ORDER BY
            a.broker_id
        """
    return sqll.read_sql(sql, conn)


def get_normal_df():
    data_dict = {
        '机构ID': {0: 10000, 1: 10000, 2: 10000, 3: 10000, 4: 10000, 5: 10000, 6: 10000, 7: 10000, 8: 10000, 9: 10000,
                 10: 10000, 11: 10000, 12: 10000, 13: 10001, 14: 10001, 15: 10002, 16: 10003, 17: 10003,
                 18: 10004, 19: 10005, 20: 10005, 21: 10005, 22: 10006, 23: 10007, 24: 10007, 25: 10007, 26: 10008,
                 27: 10008, 28: 10008,
                 29: 10009, 30: 10009, 31: 10010, 32: 10010, 33: 10011, 34: 10011, 35: 10012, 36: 10012, 37: 10012,
                 38: 10013, 39: 10013,
                 40: 10013, 41: 10014, 42: 10015, 43: 10015, 44: 10016, 45: 10016, 46: 10017, 47: 10017,
                 48: 10018, 49: 10018, 50: 10019, 51: 10019, 52: 10020, 53: 10020, 54: 10021, 55: 10021,
                 56: 10022, 57: 10022, 58: 10023, 59: 10023, 60: 10024, 61: 10024, 62: 10025, 63: 10025, 64: 10026,
                 65: 10026, 66: 10026, 67: 10027, 68: 10027},
        '机构名称': {0: '深圳交易所', 1: '深圳交易所', 2: '深圳交易所', 3: '深圳交易所', 4: '上海交易所', 5: '上海交易所',
                 6: '上海交易所', 7: '上海交易所', 8: '上海交易所', 9: '北京交易所', 10: '北京交易所', 11: '北京交易所',
                 12: '北京交易所', 13: '中信证券', 14: '中信证券', 15: '国泰君安', 16: '华泰证券',
                 17: '华泰证券', 18: '中国银河', 19: '招商证券', 20: '招商证券', 21: '招商证券', 22: '中信建投',
                 23: '国信证券', 24: '国信证券', 25: '国信证券', 26: '国元证券', 27: '国元证券', 28: '国元证券', 29: '中泰证券',
                 30: '中泰证券', 31: '长江证券', 32: '长江证券', 33: '兴业证券', 34: '兴业证券', 35: '长城证券', 36: '长城证券', 37: '长城证券',
                 38: '广发证券',
                 39: '广发证券', 40: '广发证券', 41: '中金公司',
                 42: '申万宏源', 43: '申万宏源', 44: '财通证券', 45: '财通证券', 46: '东方财富', 47: '东方财富', 48: '东兴证券', 49: '东兴证券',
                 50: '光大证券', 51: '光大证券', 52: '海通证券', 53: '海通证券', 54: '安信证券',
                 55: '安信证券', 56: '中金财富', 57: '中金财富', 58: '平安证券',
                 59: '平安证券', 60: '方正证券', 61: '方正证券', 62: '东方证券', 63: '东方证券', 64: '信达证券', 65: '信达证券', 66: '信达证券',
                 67: '东北证券', 68: '东北证券'},
        '机构代码': {0: 'EXCHANGE', 1: 'EXCHANGE', 2: 'EXCHANGE', 3: 'EXCHANGE', 4: 'EXCHANGE', 5: 'EXCHANGE',
                 6: 'EXCHANGE', 7: 'EXCHANGE', 8: 'EXCHANGE', 9: 'EXCHANGE', 10: 'EXCHANGE', 11: 'EXCHANGE',
                 12: 'EXCHANGE', 13: 'ZX', 14: 'ZX', 15: 'GTJA', 16: 'HT', 17: 'HT',
                 18: 'ZGYH', 19: 'ZS', 20: 'ZS', 21: 'ZS', 22: 'ZXJT', 23: 'GX', 24: 'GX', 25: 'GX',
                 26: 'GY', 27: 'GY', 28: 'GY', 29: 'ZT', 30: 'ZT', 31: 'CJ', 32: 'CJ', 33: 'XY', 34: 'XY', 35: 'CC',
                 36: 'CC', 37: 'CC',
                 38: 'GF', 39: 'GF', 40: 'GF', 41: 'ZJ', 42: 'SWHY',
                 43: 'SWHY', 44: 'CT', 45: 'CT', 46: 'DFCF', 47: 'DFCF', 48: 'DX', 49: 'DX', 50: 'GD', 51: 'GD',
                 52: 'HAIT', 53: 'HAIT', 54: 'AX', 55: 'AX', 56: 'ZJCF', 57: 'ZJCF',
                 58: 'PA', 59: 'PA', 60: 'FZ', 61: 'FZ', 62: 'DF', 63: 'DF', 64: 'XD', 65: 'XD', 66: 'XD', 67: 'DB',
                 68: 'DB'},
        'type': {0: '0', 1: '1', 2: '2', 3: '3', 4: '0', 5: '1', 6: '2', 7: '4', 8: '5', 9: '0', 10: '1', 11: '2',
                 12: '3', 13: '2', 14: '3', 15: '99', 16: '2', 17: '3', 18: '99', 19: '2', 20: '5', 21: '4'
            , 22: '99', 23: '5', 24: '2', 25: '4', 26: '2', 27: '4', 28: '5', 29: '2', 30: '3', 31: '2', 32: '3',
                 33: '3', 34: '2',
                 35: '2', 36: '4', 37: '5', 38: '2', 39: '4', 40: '5', 41: '99', 42: '2', 43: '3', 44: '2', 45: '3',
                 46: '2', 47: '3', 48: '2', 49: '3', 50: '2', 51: '3', 52: '2', 53: '3', 54: '2', 55: '3', 56: '2',
                 57: '3', 58: '2', 59: '3', 60: '2', 61: '3', 62: '2', 63: '3', 64: '2', 65: '4', 66: '5', 67: '2',
                 68: '3'}
    }
    return pd.DataFrame(data_dict)


def get_security_df():
    conn = pymysql.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        passwd=password,
    )
    sql = f'''
        SELECT * FROM `db-internet-biz-data`.`t_security_broker`
    '''
    return sqll.read_sql(sql, conn)


if __name__ == '__main__':
    monitoring()
