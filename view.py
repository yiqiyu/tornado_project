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
        ajax = self.get_query_argument("ajax", "")
        if not jobarea:
            self.write_error(400)
        spi = spider.AsynSpider(analysis.BasicAnalysis(), jobarea=jobarea)
        yield spi.run()
        output = spi.get_output(ajax)
        self.write(json.dumps({"type": "dist", "data": output}))


class GevCrawlerHandler(tornado.web.RequestHandler):
    @Catch.catch_and_log
    def get(self):
        jobarea = self.get_query_argument("jobarea", "")
        if not jobarea:
            self.write_error(400)
        spi = spider_gevent.AsynSpiderWithGevent(analysis.BasicAnalysis(), jobarea=jobarea)
        spi.run()
        output = spi.get_output()
        self.write(json.dumps(output))


class CollectCorpus(tornado.web.RequestHandler):
    @Catch.catch_and_log_asyn
    @gen.coroutine
    def get(self):
        db_client = mongodb.Mongodb()
        # jobareas = [db_client.getCityCode(name) for name in ("北京", "广州", "上海", "深圳")]
        jobareas = [{"jobarea": db_client.getCityCode(name)} for name in ("北京", )]
        spi = spider.MultiRequestSpiderFactory().getSpider(analysis.CorpusDBOut, jobareas)
        print "start collecting corpus"
        yield spi.run()
        print "corpus collecting finished"
        output = spi.get_output()
        res = {
            "result": output
        }
        self.write(res)


class GetTagsHandler(tornado.web.RequestHandler):
    @Catch.catch_and_log_asyn
    @gen.coroutine
    def get(self):
        jobarea = self.get_query_argument("jobarea", "")
        key = self.get_query_argument("key", "")
        ajax = self.get_query_argument("ajax", "")
        if not jobarea or not key:
            self.write_error(400)
        print "start collecting tags"
        spi = spider.AsynSpider(analysis.IndustryTagsAnalysis(), jobarea=jobarea, keyword=key)
        yield spi.run()
        print "tags collecting finished"
        output = spi.get_output(ajax)
        self.write(json.dumps({"type": "tags", "data": output}))

