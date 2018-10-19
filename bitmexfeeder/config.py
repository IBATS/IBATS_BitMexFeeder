#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/27 10:49
@File    : config.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging


class ConfigBase:

    # 交易所名称
    MARKET_NAME = 'bitmex'

    # api configuration
    # https://testnet.bitmex.com/app/apiKeys
    TEST_NET = True
    EXCHANGE_PUBLIC_KEY = "K5DaKlClbXg_TQn5lEGOswd8"
    EXCHANGE_SECRET_KEY = "QQwPpUpCUcJwtqFIsDXevMqhEPUM3eanZUnzlSpYGqaLIbph"

    # mysql db info
    DB_HANDLER_ENABLE = True
    DB_SCHEMA_MD = 'md_bitmex'
    DB_URL_DIC = {
        DB_SCHEMA_MD: 'mysql://mg:Abcd1234@localhost/' + DB_SCHEMA_MD
    }

    # redis info
    REDIS_PUBLISHER_HANDLER_ENABLE = False
    REDIS_INFO_DIC = {'REDIS_HOST': 'localhost',  # '192.168.239.131'
                      'REDIS_PORT': '6379',
                      }

    # evn configuration
    LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(filename)s.%(funcName)s:%(lineno)d|%(message)s'

    def __init__(self):
        """
        初始化一些基本配置信息
        """

        # log settings
        logging_config = dict(
            version=1,
            formatters={
                'simple': {
                    'format': ConfigBase.LOG_FORMAT}
            },
            handlers={
                'file_handler':
                    {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'filename': 'logger.log',
                        'maxBytes': 1024 * 1024 * 10,
                        'backupCount': 5,
                        'level': 'DEBUG',
                        'formatter': 'simple',
                        'encoding': 'utf8'
                    },
                'console_handler':
                    {
                        'class': 'logging.StreamHandler',
                        'level': 'DEBUG',
                        'formatter': 'simple'
                    }
            },

            root={
                'handlers': ['console_handler', 'file_handler'],  #
                'level': logging.DEBUG,
            }
        )
        # 设置日志输出级别
        # logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
        # logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
        # logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_min1_tick_bc').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_min1_bc').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_min60_bc').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_daily_bc').setLevel(logging.INFO)
        # logging.getLogger('MDFeeder').setLevel(logging.INFO)
        # logging.getLogger('md_min1_bc').setLevel(logging.INFO)
        # logging.getLogger('md_min1_tick_bc').setLevel(logging.INFO)
        from logging.config import dictConfig
        dictConfig(logging_config)


# 测试配置（测试行情库）
config = ConfigBase()
# 生产配置
# config = ConfigProduct()
