#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import multiprocessing

from tornado.web import gen
from tornado import httpclient
from tornado.queues import Queue
import urllib
from lxml import etree

from analysis import BasicAnalysis, Analysis
from settings import *


LIST_URL = "http://api.51job.com/api/job/search_job_list.php?"
DETAIL_URL = "http://api.51job.com/api/job/get_job_info.php?"

CONCURRENCY = 50
CPU_COUNT = multiprocessing.cpu_count()
PAGE_SIZE = 20

LIST_QUERY = {
    "postchannel": "0000",
    "jobarea": "030700",
    "pageno": 1,
    "pagesize": PAGE_SIZE,
    "accountid": "",
    "key": "",
    "keyword": "java",
    "kewwordtype": 2,
    "productname": "51job",
    "partner": PARTNER,
    "uuid": UUID,
    "version": 702,
    "guid": GUID
}

DETAIL_QUERY = {
    "jobid": "77974326",
    "accountid": "",
    "key": "",
    "isad": "",
    "from": "",
    "productname": "51job",
    "partner": PARTNER,
    "uuid": UUID,
    "version": 702,
    "guid": GUID
}

HEADERS = {"User-Agent": "51job-android-client",
           "Accept-Encoding": "gzip",
           "Connection": "close",
           "Host": "api.51job.com"
           }


class SpiderException(Exception):
    pass


class MySpider(object):
    def __init__(self, out=BasicAnalysis(), **kwargs):
        self._out = out
        self.list_query = LIST_QUERY.copy()
        for k,v in kwargs.items():
            if k in self.list_query.keys():
                self.list_query[k] = v.encode("utf-8")

    def assign_jobs(self, jobs):
        raise NotImplementedError

    def get_output(self, ajax=False):
        if isinstance(self._out, Analysis):
            if self._out.has_finished():
                return self._out.get_results(ajax)
            else:
                raise SpiderException("Spider has not finished!")
        else:
            return self._out.get_results(ajax)

    @staticmethod
    def createJob(args):
        list_query = LIST_QUERY.copy()
        for k,v in args.items():
            if k in list_query.keys():
                list_query[k] = v
        return LIST_URL + urllib.urlencode(list_query)


class AsynSpider(MySpider):
    def __init__(self, out, **kwargs):
        super(AsynSpider, self).__init__(out, **kwargs)
        self.client = httpclient.AsyncHTTPClient()
        self.q = Queue()
        self.fetching, self.fetched = set(), set()

    def assign_jobs(self, jobs):
        for job in jobs:
            self.q.put(job)

    @gen.coroutine
    def run(self):
        if self.q.empty():
            url = LIST_URL + urllib.urlencode(self.list_query)
            self.q.put(url)
        for _ in range(CONCURRENCY):
            self.worker()
        yield self.q.join()
        assert self.fetching == self.fetched
        # print len(self.fetched)
        if isinstance(self._out, Analysis):
            self._out.finish()

    @gen.coroutine
    def worker(self):
        while True:
            yield self.fetch_url()

    @gen.coroutine
    def fetch_url(self):
        current_url = yield self.q.get()
        try:
            if current_url in self.fetching:
                return
            self.fetching.add(current_url)
            request = httpclient.HTTPRequest(current_url, headers=HEADERS)
            resp = yield self.client.fetch(request)
            self.fetched.add(current_url)
            xml = etree.fromstring(resp.body)
            has_total_count = xml.xpath("//totalcount/text()")
            if has_total_count:  # 非空证明为列表，否则为详细页
                total_count = int(has_total_count[0])
                if total_count == 0:
                    return  # 列表跨界
                if self.list_query["pageno"] == 1:
                    pageno = 2
                    while pageno < 10:
                    # while pageno <= total_count / PAGE_SIZE:
                        self.list_query["pageno"] = pageno
                        next_list_url = LIST_URL + urllib.urlencode(self.list_query)
                        self.q.put(next_list_url)
                        # logging.info(next_list_url)
                        pageno += 1
                job_ids = xml.xpath("//jobid/text()")
                job_detail_urls = []
                for ID in job_ids:
                    new_detail_query = DETAIL_QUERY.copy()
                    new_detail_query["jobid"] = ID
                    job_detail_urls.append(DETAIL_URL + urllib.urlencode(new_detail_query))
                for detail_url in job_detail_urls:
                    self.q.put(detail_url)
                    # logging.info(detail_url)

            else:
                self._out.collect(xml)
        finally:
            self.q.task_done()


class MultiRequestSpiderFactory(object):
    def getSpider(self, out, args):
        """
        :param args: 为一个数组，每个数组为一个字典，必须有键kwargs，kwargs为对应的spider的请求参数字典
        :return: 返回相应的spider实例
        """
        if len(args) == 1:

            return AsynSpider(out(), **args[0].get("kwargs", {}))
        elif len(args) > 1:
            spider = AsynSpider(out())
            spider.assign_jobs([MySpider.createJob(arg) for arg in args])
            return spider
