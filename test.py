# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import requests
from lxml import etree
from location_dict_scrawler import PARTNER, UUID, GUID
import spider_gevent
import analysis

LIST_URL = "http://api.51job.com/api/job/search_job_list.php"
DETAIL_URL = "http://api.51job.com/api/job/get_job_info.php"

LIST_QUERY = {
    "postchannel": "0000",
    "jobarea": "030700",
    "pageno": 1,
    "pagesize": 20,
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
    "guid":GUID
}

HEADERS = {"User-Agent": "51job-android-client",
           "Accept-Encoding": "gzip",
           "Connection": "close",
           "Host": "api.51job.com"
           }

# response = requests.get(LIST_URL, LIST_QUERY, headers=HEADERS)
# # response = requests.get(DETAIL_URL, DETAIL_QUERY, headers=HEADERS)
# xml = etree.fromstring(response.content)
# print xml.xpath("//totalcount/text()")


spi = spider_gevent.MultiProcessAsynSpider(analysis.BasicAnalysis(), jobarea="030700")
spi.run()
output = spi.get_output()
print output.get_results()



