#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:47
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from sqlalchemy import create_engine
from ibats_bitmex_feeder.config import config
import logging

logger = logging.getLogger()
engines = {}
for key, url in config.DB_URL_DIC.items():
    logger.debug('加载 engine %s: %s', key, url)
    engines[key] = create_engine(url)

engine_md = engines[config.DB_SCHEMA_MD]
