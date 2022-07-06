#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author lbj
# 2022/6/23 13:55
# pip3 install DBUtils==1.3

import pymysql
from dbutils.pooled_db import PooledDB
from utils.logs_utils import logger
from configparser import ConfigParser
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, 'config/config.ini')

cf = ConfigParser()
cf.read(full_path)

host = cf.get('mysql', 'host')
port = cf.getint('mysql', 'port')
username = cf.get('mysql', 'username')
password = cf.get('mysql', 'password')
schema = cf.get('mysql', 'schema')
charset = cf.get('mysql', 'charset')

global_pool = PooledDB(
    creator=pymysql,
    maxconnections=200,
    mincached=4,
    maxcached=20,
    maxshared=0,
    blocking=True,
    setsession=[],
    ping=5,
    host=host,
    port=port,
    user=username,
    password=password,
    database=schema,
    charset=charset)

if __name__ == '__main__':
    conn = global_pool.connection()
    cursor = conn.cursor()
    print(cursor)
