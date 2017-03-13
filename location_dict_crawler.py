# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from lxml import etree
import requests
import json

PARTNER = "bceb5aaaa832653d5b26e2756e74bb64"
UUID = "e5eb7bf894f10fa2a4a3982a0dcf8609"
GUID = "133090b8365c94f3f2bf7848540ca784"
MAIN_URL = "http://appapi.51job.com/api/datadict/get_dd_jobarea.php"
# URL = "http://appapi.51job.com/api/datadict/get_dd_jobarea.php?ddtype=dd_jobarea&code=&language=c&productname=51job&partner=bceb5aaaa832653d5b26e2756e74bb64&uuid=e5eb7bf894f10fa2a4a3982a0dcf8609&version=702&guid=133090b8365c94f3f2bf7848540ca784"

query = {
"ddtype": "dd_jobarea",
"code": "",
"language": "c",
"productname": "51job",
"partner": PARTNER,
"uuid": UUID,
"version": 702,
"guid": GUID
}

response = requests.get(MAIN_URL, query)
xml = etree.fromstring(response.content)
city_code = {}
for item in xml.xpath("//item"):
    code = item.xpath("code/text()")[0]
    city = item.xpath("value/text()")[0].encode("utf-8")
    query["code"] = code
    resp = requests.get(MAIN_URL,query)
    content_ = (etree.fromstring(resp.content)).xpath("//item")
    if len(content_):
        for item_ in content_:
            code = item_.xpath("code/text()")[0]
            city = item_.xpath("value/text()")[0].encode("utf-8")
            city_code[city] = code
    else:
        city_code[city] = code

with open("city_code", "a+") as f:
    # for city, code in city_code.items():
    #     f.write(city+"\t"+code+"\n")
    json.dump(city_code, f)
