# coding=utf-8
from pymongo import MongoClient


import logging
import logging.config
import json
import time
import re
from bson.objectid import ObjectId
import hashlib

import logManager



class Mongodb: 
	serverIP = ''
	port = 27017

	__conn = None
	__db_config = None
	__logger = logManager.getLogger('Mongodb') 
	

	def __init__(self,serverIP,port,user="sa",passwd="123456qwerty") :
		self.serverIP = serverIP
		self.port = port
		if self.__conn is None :
			try :
				self.__conn = MongoClient(serverIP,port,w=0)
				self.__conn.admin.authenticate(user,passwd)
				self.__logger.info("Open conneciton to [%s:%d]",serverIP,port)
			except Exception as e :
				self.__logger.error(e)
				raise Exception(e)
		if self.__db_config is None :
			self.__db_config = self.__conn.config

	def getDomainList(self,queryJson) :
		try :
			collection = self.__db_config.domain
			cursor = collection.find(queryJson)
			self.__logger.info("Get domains[%d]",cursor.count())
			domainList = [] 
			for domain in cursor :
				domainList.append(domain)
			return domainList 
		except Exception as e :
			self.__logger.error(e)
			raise Exception(e)

	def isStored(self,dbname,collection,docID,idType=0):
		try :
			db = self.__conn[dbname]
			collection = db[collection]
			if idType == 0 :
				doc = collection.find_one({"_id":docID})
			else:
				doc = collection.find_one({"_id":ObjectId(docID)})
			if doc is None :
				return False 
			else:
				return True
		except Exception as e :
			self.__logger.error(e)
			raise Exception(e)
	
	
        def getUserList(self,domain,number) :
                try :
                        collection = self.__db_config.user
                        cursor = collection.find({"domain":domain}).limit(number)
                        userList = []
                        for user in cursor :
                                userList.append(str(user["user"]))
                        return userList
                except Exception as e :
                        self.__logger.error(e)
                        raise Exception(e)

	def getProfileKW(self):
		try :
                        collection = self.__db_config.keyword
                        cursor = collection.find({"_id":1})
                        kwList = []
                        for record in cursor :
				kwList = record["kw"]
                        return kwList
                except Exception as e :
                        self.__logger.error(e)
                        raise Exception(e)


        def getRules(self,domain) :
                try :
                        collection = self.__db_config.rule
                        cursor = collection.find({"domain":domain})
                        ruleList = []
                        for rule in cursor :
                                ruleList.append(rule)
                        return ruleList
                except Exception as e :
                        self.__logger.error(e)
                        raise Exception(e)


	def getProxyList(self,number) :
		try :
		 	collection = self.__db_config.proxy
			cursor = collection.find().sort("ts",-1).limit(number)
			proxyList = []
                        for proxy in cursor :
                                proxyList.append(proxy)
			return proxyList
                except Exception as e :
                        self.__logger.error(e)
                        raise Exception(e)

	def bulkInsert(self,dbname,collection,docList) :
		try :
                        db = self.__conn[dbname]
                        collection = db[collection]
			result = collection.insert_many(docList,ordered=False)
			self.__logger.info("Bulk Insert collection[%s][%d]",collection,len(docList))
			return result
		except Exception as e  :
			self.__logger.error(e)
			raise Exception(e)

	def bulkUpsert(self,dbname,collection,docList) :
                try :
                        db = self.__conn[dbname]
                        collection = db[collection]
                        result = collection.insert_many(docList,ordered=False)
                        self.__logger.info("Bulk Insert collection[%s][%d]",collection,len(docList))
                except Exception as e  :
                        self.__logger.error(e)
                        raise Exception(e)



	
#if __name__ == '__main__':	
#	db = Mongodb("mongodb",27017)
	#domainList = db.getDomainList({"accessByCH":1})
#	print domainList
#	docList = []
#	_id= hashlib.md5("http://www.124.com1").hexdigest()
#	doc = {"title":"this is tes111111","_id":"2222222"}
#	docList.append(doc)
#	print db.bulkInsert("news","test.com",docList)
#	proxyList = db.getProxyList(1) 
#	print proxyList
	
	
	
	
	
