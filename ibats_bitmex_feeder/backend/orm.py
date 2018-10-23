#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:02
@File    : orm.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from sqlalchemy import Column, Integer, String, UniqueConstraint, TIMESTAMP, MetaData, Table
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.ext.declarative import declarative_base
from ibats_common.utils.db import with_db_session, get_db_session
from ibats_bitmex_feeder.backend import engine_md
from ibats_bitmex_feeder.config import config
import logging

logger = logging.getLogger()
BaseModel = declarative_base()
table_model_dic = {}
INSTRUMENT_INFO_TABLE_NAME = 'bitmex_instrument'
# 类型：type(instrument_info_table) :  <class 'sqlalchemy.sql.schema.Table'>
# 用法：session.execute(instrument_info_table.count()).scalar()
instrument_info_table = None


class MDMin1(BaseModel):
    __tablename__ = 'md_bitmext_min1'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDMin1Temp(BaseModel):
    __tablename__ = 'md_bitmext_min1_temp'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDMin5(BaseModel):
    __tablename__ = 'md_bitmext_min5'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDMin5Temp(BaseModel):
    __tablename__ = 'md_bitmext_min5_temp'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDHour1(BaseModel):
    __tablename__ = 'md_bitmext_hour1'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDHour1Temp(BaseModel):
    __tablename__ = 'md_bitmext_hour1_temp'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDDaily(BaseModel):
    __tablename__ = 'md_bitmext_daily'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


class MDDailyTemp(BaseModel):
    __tablename__ = 'md_bitmext_daily_temp'
    timestamp = Column(TIMESTAMP, primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(DOUBLE)
    high = Column(DOUBLE)
    low = Column(DOUBLE)
    close = Column(DOUBLE)
    trades = Column(DOUBLE)
    volume = Column(DOUBLE)
    vwap = Column(DOUBLE)
    lastSize = Column(DOUBLE)
    turnover = Column(DOUBLE)
    homeNotional = Column(DOUBLE)
    foreignNotional = Column(DOUBLE)


def init(alter_table=False):
    BaseModel.metadata.create_all(engine_md)
    if alter_table:
        with with_db_session(engine=engine_md) as session:
            for table_name, _ in BaseModel.metadata.tables.items():
                sql_str = f"show table status from {config.DB_SCHEMA_MD} where name=:table_name"
                row_data = session.execute(sql_str, params={'table_name': table_name}).first()
                if row_data is None:
                    continue
                if row_data[1].lower() == 'myisam':
                    continue

                logger.info('修改 %s 表引擎为 MyISAM', table_name)
                sql_str = "ALTER TABLE %s ENGINE = MyISAM" % table_name
                session.execute(sql_str)

    logger.info("所有表结构建立完成")


def dynamic_load_table_model():
    """
    动态加载数据库中所有表model，保存到 table_model_dic
    :return:
    """
    metadata = MetaData(engine_md)
    sql_str = """select TABLE_NAME from information_schema.TABLES where table_schema=:table_schema"""
    with with_db_session(engine_md) as session:
        table_name_list = [row[0] for row in session.execute(
            sql_str, params={'table_schema': config.DB_SCHEMA_MD}).fetchall()]
    table_name_list_len = len(table_name_list)
    for num, table_name in enumerate(table_name_list, start=1):
        model = Table(table_name, metadata, autoload=True)
        table_model_dic[table_name] = model
        logger.debug('%d/%d) load table %s', num, table_name_list_len, table_name)
        if INSTRUMENT_INFO_TABLE_NAME == table_name:
            global instrument_info_table
            instrument_info_table = model


if __name__ == "__main__":
    init(True)
    dynamic_load_table_model()
