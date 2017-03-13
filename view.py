#!/usr/bin/env python
# -*- coding:utf-8 -*-

from functools import wraps
import json
import traceback
import logging

import tornado.web
from tornado.web import gen
import tornado.ioloop

from common import mongodb
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


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("main.html")

    def post(self, *args, **kwargs):
        DB_CLIENT = mongodb.Mongodb()
        name = self.get_body_argument("name", "全国")
        result = DB_CLIENT.getCityCode(name.encode("utf-8"))
        if result:
            return self.write(result)


class CrawlerHandler(tornado.web.RequestHandler):
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


class GevCrawlerHandler(tornado.web.RequestHandler):
    @Catch.catch_and_log
    def get(self):
        jobarea = self.get_query_argument("jobarea", "")
        if not jobarea:
            self.write_error(400)
        spi = spider_gevent.AsynSpiderWithGevent(analysis.BasicAnalysis(), jobarea=jobarea)
        spi.run()
        output = spi.get_output()
        self.write(json.dumps(output.get_results()))
