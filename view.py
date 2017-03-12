#!/usr/bin/env python
# -*- coding:utf-8 -*-

from functools import wraps
import json
import traceback
import logging

import tornado.web
from tornado.web import gen
import tornado.ioloop

import spider
import spider_gevent
import analysis


class Catch(object):
    @staticmethod
    def catch_and_log_asyn(func):
        @gen.coroutine
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                yield func(*args, **kwargs)
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
        return wrapper

    @staticmethod
    def catch_and_log(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
        return wrapper



class TestHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        import multiprocessing
        import time
        a = multiprocessing.Array("i", 10)

        def worker():
            time.sleep(3)
            a[0] += 1

        processes = [multiprocessing.Process(None, worker) for _ in range(4)]
        try:
            for p in processes:
                p.daemon = True
                p.start()
            for p in processes:
                p.join()
            self.write(str(a[0]))
        finally:
            for p in processes:
                p.terminate()



class ScrawlerHandler(tornado.web.RequestHandler):
    @Catch.catch_and_log_asyn
    @gen.coroutine
    def get(self):
        jobarea = self.get_query_argument("jobarea", "")
        if not jobarea:
            self.write_error(400)
        spi = spider.AsynSpider(analysis.BasicAnalysis(), jobarea=jobarea)
        yield spi.run()
        output = spi.get_output()
        self.write(json.dumps(output.get_results()))


class GevScralerHandler(tornado.web.RequestHandler):
    @Catch.catch_and_log
    def get(self):
        jobarea = self.get_query_argument("jobarea", "")
        if not jobarea:
            self.write_error(400)
        spi = spider_gevent.AsynSpiderWithGevent(analysis.BasicAnalysis(), jobarea=jobarea)
        spi.run()
        output = spi.get_output()
        self.write(json.dumps(output.get_results()))
