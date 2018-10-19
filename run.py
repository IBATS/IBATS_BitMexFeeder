#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/12 20:38
@File    : run.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import time
import logging
from bitmexfeeder.backend.orm import init
from bitmexfeeder.feeder import start_feeder
logger = logging.getLogger()


if __name__ == "__main__":
    init(True)

    # while True:
    supplier = start_feeder(init_symbols=True, do_fill_history=True)
    try:
        while supplier.is_working:
            time.sleep(5)
    except KeyboardInterrupt:
        logger.warning('Feeder 终止...')
    finally:
        supplier.is_working = False
        supplier.stop()
        supplier.join()
        logger.warning('子线程已经结束')
