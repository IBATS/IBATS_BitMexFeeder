#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 13:02
@File    : orm.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from sqlalchemy import Column, Integer, String, UniqueConstraint, TIMESTAMP
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.ext.declarative import declarative_base
from ibats_common.utils.db import with_db_session
from bitmexfeeder.backend import engine_md
from bitmexfeeder.config import config
import logging
logger = logging.getLogger()
BaseModel = declarative_base()


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


if __name__ == "__main__":
    init()
