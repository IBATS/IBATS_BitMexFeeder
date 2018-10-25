#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/10/25 17:42
@File    : other_tables.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from ibats_bitmex_feeder.backend.orm import dynamic_load_table_model, table_model_dic


if len(table_model_dic) == 0:
    dynamic_load_table_model()

INSTRUMENT_INFO_TABLE_NAME = 'bitmex_instrument'
# 类型：type(instrument_info_table) :  <class 'sqlalchemy.sql.schema.Table'>
# 用法：session.execute(instrument_info_table.count()).scalar()
instrument_info_table = table_model_dic[INSTRUMENT_INFO_TABLE_NAME]
