#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import multiprocessing

from tornado.web import gen
from tornado import httpclient
from tornado.queues import Queue
import urllib
from lxml import etree

from analysis import BasicAnalysis

#IDs in query string, could change in any time
PARTNER = "bceb5aaaa832653d5b26e2756e74bb64"
UUID = "e5eb7bf894f10fa2a4a3982a0dcf8609"
GUID = "133090b8365c94f3f2bf7848540ca784"

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
        self.list_query = LIST_QUERY.copy()
        self._out = out
        for k,v in kwargs.items():
            if k in self.list_query.keys():
                self.list_query[k] = v

    def get_output(self):
        if self._out.has_finished():
            return self._out
        else:
            raise SpiderException("Spider is not finished!")


class AsynSpider(MySpider):
    def __init__(self, out, **kwargs):
        super(AsynSpider, self).__init__(out, **kwargs)
        self.client = httpclient.AsyncHTTPClient()
        self.q = Queue()
        self.fetching, self.fetched = set(), set()

    @gen.coroutine
    def run(self):
        url = LIST_URL + urllib.urlencode(self.list_query)

        self.q.put(url)
        for _ in range(CONCURRENCY):
            self.worker()
        yield self.q.join()
        assert self.fetching == self.fetched
        # print len(self.fetched)
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


