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
from ibats_common.config import ConfigBase as ConBase, update_db_config
from ibats_common.common import ExchangeName

logger = logging.getLogger(__name__)


class ConfigBase(ConBase):

    # 交易所名称
    MARKET_NAME = ExchangeName.BitMex.name

    # api configuration
    # https://testnet.bitmex.com/app/apiKeys
    TEST_NET = True
    EXCHANGE_PUBLIC_KEY = "K5DaKlClbXg_TQn5lEGOswd8"
    EXCHANGE_SECRET_KEY = "QQwPpUpCUcJwtqFIsDXevMqhEPUM3eanZUnzlSpYGqaLIbph"

    # mysql db info
    DB_HANDLER_ENABLE = True
    DB_SCHEMA_MD = 'md_bitmex'
    DB_URL_DIC = {
        DB_SCHEMA_MD: 'mysql://m*:****@localhost/' + DB_SCHEMA_MD
    }

    # redis info
    REDIS_PUBLISHER_HANDLER_ENABLE = False
    REDIS_INFO_DIC = {'REDIS_HOST': 'localhost',  # '192.168.239.131'
                      'REDIS_PORT': '6379',
                      }

    def __init__(self):
        """
        初始化一些基本配置信息
        """
        # 设置日志输出级别
        # logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
        # logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
        # logging.getLogger('DBHandler->md_min1_tick_bc').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_min1_bc').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_min60_bc').setLevel(logging.INFO)
        # logging.getLogger('DBHandler->md_daily_bc').setLevel(logging.INFO)
        # logging.getLogger('MDFeeder').setLevel(logging.INFO)
        # logging.getLogger('md_min1_bc').setLevel(logging.INFO)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
        logging.getLogger('bravado_core.model').setLevel(logging.INFO)
        logging.getLogger('bravado.client').setLevel(logging.INFO)
        logging.getLogger('bravado_core.resource').setLevel(logging.INFO)
        logging.getLogger('swagger_spec_validator.ref_validators').setLevel(logging.INFO)
        logging.getLogger('swagger_spec_validator.validator20').setLevel(logging.INFO)


# 测试配置（测试行情库）
config = ConfigBase()
update_db_config(config.DB_URL_DIC)


def update_config(config_new: ConfigBase, update_db=True):
    global config
    config = config_new
    logger.info('更新默认配置信息 %s < %s', ConfigBase, config_new.__class__)
    if update_db:
        update_db_config(config.DB_URL_DIC)
