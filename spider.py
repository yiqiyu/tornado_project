#!/usr/bin/env python
# -*- coding:utf-8 -*-

class MySpider(object):

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