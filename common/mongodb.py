# coding=utf-8
from pymongo import MongoClient
import requests
from lxml import etree
from bson.objectid import ObjectId

import logManager
from spider import PARTNER, UUID, GUID


class Mongodb(object):
    serverIP = ''
    port = 27017

    __conn = None
    __db_config = None
    __logger = logManager.getLogger('Mongodb') 

    def __init__(self, serverIP="127.0.0.1", port=27017, user="sa", passwd="123456qwerty"):
        self.serverIP = serverIP
        self.port = port
        if self.__conn is None:
            try :
                self.__conn = MongoClient(serverIP, port, w=0)
                # self.__conn.admin.authenticate(user, passwd)
                self.__logger.info("Open conneciton to [%s:%d]", serverIP, port)
            except Exception as e :
                self.__logger.error(e)
                raise Exception(e)
        if self.__db_config is None :
            self.__db_config = self.__conn.config

    def getCityCode(self, name):
        try:
            result = self.__conn["51job"]["cityCode"].find_one({"name": name})
            return result["code"]
        except Exception as e:
            self.__logger.error(e)
            raise Exception(e)

    def renewCityCode(self):
        collection = self.__conn["51job"]["cityCode"]
        if collection.count() > 0:
            collection.delete_many({})
        url = "http://appapi.51job.com/api/datadict/get_dd_jobarea.php"
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

        response = requests.get(url, query)
        xml = etree.fromstring(response.content)
        city_code = []
        for item in xml.xpath("//item"):
            code = item.xpath("code/text()")[0]
            city = item.xpath("value/text()")[0].encode("utf-8")
            query["code"] = code
            resp = requests.get(url, query)
            content_ = (etree.fromstring(resp.content)).xpath("//item")
            if len(content_):
                for item_ in content_:
                    code = item_.xpath("code/text()")[0]
                    city = item_.xpath("value/text()")[0].encode("utf-8")
                    city_code.append({"name": city, "code": code})
            else:
                city_code.append({"name": city, "code": code})
        collection.insert_many(city_code)

    @property
    def conn(self):
        return self.__conn

    #if __name__ == '__main__':
#    db = Mongodb("mongodb",27017)
    #domainList = db.getDomainList({"accessByCH":1})
#    print domainList
#    docList = []
#    _id= hashlib.md5("http://www.124.com1").hexdigest()
#    doc = {"title":"this is tes111111","_id":"2222222"}
#    docList.append(doc)
#    print db.bulkInsert("news","test.com",docList)
#    proxyList = db.getProxyList(1) 
#    print proxyList
    
    
    
    
    
