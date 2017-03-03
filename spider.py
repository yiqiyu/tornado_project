#!/usr/bin/env python
# -*- coding:utf-8 -*-

from tornado.web import gen
from tornado import httpclient
import tornado.web
from tornado.queues import Queue
import urllib
from lxml import etree


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


class MySpider(object):
    def __init__(self, analysis):
        self.client = httpclient.AsyncHTTPClient()
        self.q = Queue()
        self.analysis = analysis()

    @gen.coroutine
    def run(self, jobarea):
        list_query = LIST_QUERY.copy()
        list_query["jobarea"] = jobarea
        url = LIST_URL + urllib.urlencode(list_query)

        fetching, fetched = set(), set()

        @gen.coroutine
        def fetch_url():
            current_url = yield self.q.get()
            try:
                if current_url in fetching:
                    return
                fetching.add(current_url)
                request = httpclient.HTTPRequest(current_url, headers=HEADERS)
                resp = yield self.client.fetch(request)
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
                            self.q.put(next_list_url)
                            logging.info(next_list_url)
                            pageno += 1
                    job_ids = xml.xpath("//jobid/text()")
                    job_detail_urls = []
                    for ID in job_ids:
                        new_detail_query = DETAIL_QUERY.copy()
                        new_detail_query["jobid"] = ID
                        job_detail_urls.append(DETAIL_URL+urllib.urlencode(new_detail_query))
                    for detail_url in job_detail_urls:
                        self.q.put(detail_url)
                        logging.info(detail_url)

                else:
                    self.analysis.execute(xml)

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
        for value in self.analysis.categories.values():
            total_collect += value
        self.analysis.categories["total"] = total_collect
