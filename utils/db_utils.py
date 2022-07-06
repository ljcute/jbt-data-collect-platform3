#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# author yanpan
# 2022/06/28 11:11
# pip3 install DBUtils==1.3
import os
from configparser import ConfigParser
from dbutils.pooled_db import PooledDB
import pymysql

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, 'D:\jbt-data-collect-platform\config\config.ini')

cf = ConfigParser()
cf.read(full_path)

host = cf.get('mysql', 'host')
port = cf.getint('mysql', 'port')
username = cf.get('mysql', 'username')
password = cf.get('mysql', 'password')
schema = cf.get('mysql', 'schema')
charset = cf.get('mysql', 'charset')
