#!/usr/bin/env python
# -*- coding:utf-8 -*-
from spider import *
from gevent import monkey; monkey.patch_all()
import gevent
from gevent.queue import JoinableQueue
import multiprocessing
import requests


class AsynSpiderWithGevent(MySpider):
    def __init__(self, out=BasicAnalysis(), **kwargs):
        super(AsynSpiderWithGevent, self).__init__(out, **kwargs)
        self.q = JoinableQueue()
        self.fetching, self.fetched = set(), set()

    def assign_jobs(self, jobs):
        for job in jobs:
            self.q.put(job)

    def run(self):
        if self.q.empty():
            url = LIST_URL + urllib.urlencode(self.list_query)
            self.q.put(url)
        for _ in range(CONCURRENCY):
            gevent.spawn(self.worker)
        self.q.join()
        assert self.fetching == self.fetched
        self._out.finish()

    def worker(self):
        while True:
            self.fetch_url()

    def fetch_url(self):
        current_url = self.q.get()
        try:
            if current_url in self.fetching:
                return
            self.fetching.add(current_url)
            resp = requests.get(current_url, headers=HEADERS)
            self.fetched.add(current_url)
            xml = etree.fromstring(resp.content)
            has_total_count = xml.xpath("//totalcount/text()")
            if has_total_count:  # 非空证明为列表，否则为详细页
                total_count = int(has_total_count[0])
                if total_count == 0:
                    return  # 列表跨界
                if self.list_query["pageno"] == 1:
                    pageno = 2
                    # while pageno < 10:
                    while pageno <= total_count / PAGE_SIZE:
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


class MultiProcessAsynSpider(MySpider):
    def get_all_lists(self):
        spider = AsynSpiderWithGevent(self._out)
        url = LIST_URL + urllib.urlencode(self.list_query)
        spider.assign_jobs([url])
        spider.fetch_url()
        results = [url]
        while not spider.q.empty():
            results.append(spider.q.get())
        return results

    def run(self):
        results = multiprocessing.Queue()
        all_lists = self.get_all_lists()

        def distribute(urls):
            spider = AsynSpiderWithGevent()
            spider.assign_jobs(urls)
            spider.run()
            results.put(spider.get_output())

        processes = [multiprocessing.Process(None, distribute, args=(all_lists[i::4],)) for i in range(CPU_COUNT)]
        for p in processes:
            p.start()
        for p in processes:
            p.join()
        while not results.empty():
            self._out.combine(results.get())
        self._out.finish()



