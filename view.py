#!/usr/bin/env python
# -*- coding:utf-8 -*-

from functools import wraps
import json
import traceback
import logging

from tornado.web import gen
from tornado import httpclient
import tornado.web
from tornado.queues import Queue
import urllib
from lxml import etree

# import mongodb


#IDs in query string, could change in any time
PARTNER = "bceb5aaaa832653d5b26e2756e74bb64"
UUID = "e5eb7bf894f10fa2a4a3982a0dcf8609"
GUID = "133090b8365c94f3f2bf7848540ca784"

LIST_URL = "http://api.51job.com/api/job/search_job_list.php?"
DETAIL_URL = "http://api.51job.com/api/job/get_job_info.php?"

CONCURRENCY = 50
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
    "guid":GUID
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
        client = httpclient.AsyncHTTPClient()
        jobarea = self.get_query_argument("jobarea", "")
        list_query = LIST_QUERY.copy()
        list_query["jobarea"] = jobarea
        url = LIST_URL + urllib.urlencode(list_query)
        print url
        request = httpclient.HTTPRequest(url, headers=HEADERS)
        resp = yield client.fetch(request)
        self.write(resp.body)


class ScrawlerHandler(tornado.web.RequestHandler):
    def basic_analysis(self, cat, xml):
        cats = xml.xpath("//indtype1/text()")
        cats.extend(xml.xpath("//indtype2/text()"))
        for c in cats:
            c = c.encode("utf-8")
            if c in cat:
                cat[c] += 1
            else:
                cat.setdefault(c, 0)

    @gen.coroutine
    def get(self):
        try:
            client = httpclient.AsyncHTTPClient()
            q = Queue()
            categories = {}

            jobarea = self.get_query_argument("jobarea", "")
            if not jobarea:
                self.write_error(400)

            list_query = LIST_QUERY.copy()
            list_query["jobarea"] = jobarea
            url = LIST_URL + urllib.urlencode(list_query)

            fetching, fetched = set(), set()

            @gen.coroutine
            def fetch_url():
                current_url = yield q.get()
                try:
                    if current_url in fetching:
                        return
                    fetching.add(current_url)
                    request = httpclient.HTTPRequest(current_url, headers=HEADERS)
                    resp = yield client.fetch(request)
                    fetched.add(current_url)
                    xml = etree.fromstring(resp.body)
                    has_total_count = xml.xpath("//totalcount/text()")
                    if has_total_count:         #非空证明为列表，否则为详细页
                        total_count = int(has_total_count[0])
                        if total_count == 0:
                            return      #列表跨界
                        if list_query["pageno"] == 1:
                            pageno = 2
                            # while pageno < 10:
                            while pageno <= total_count / PAGE_SIZE:
                                list_query["pageno"] = pageno
                                next_list_url = LIST_URL + urllib.urlencode(list_query)
                                q.put(next_list_url)
                                logging.info(next_list_url)
                                pageno += 1
                        job_ids = xml.xpath("//jobid/text()")
                        job_detail_urls = []
                        for ID in job_ids:
                            new_detail_query = DETAIL_QUERY.copy()
                            new_detail_query["jobid"] = ID
                            job_detail_urls.append(DETAIL_URL+urllib.urlencode(new_detail_query))
                        for detail_url in job_detail_urls:
                            q.put(detail_url)
                            logging.info(detail_url)

                    else:
                        self.basic_analysis(categories, xml)

                finally:
                    q.task_done()

            @gen.coroutine
            def worker():
                while True:
                    yield fetch_url()

            q.put(url)
            for _ in range(CONCURRENCY):
                worker()
            yield q.join()
            assert fetching == fetched
            print len(fetched)

            total_collect = 0
            for value in categories.values():
                total_collect += value
            categories["total"] = total_collect
            self.write(json.dumps(categories))
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
