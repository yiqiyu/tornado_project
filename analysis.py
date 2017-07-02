#!/usr/bin/env python
# -*- coding:utf-8 -*-
from common import mongodb
import traceback
import time
import math
from operator import itemgetter

import jieba
import jieba.analyse as jieba_analyse
import my_tfidf
import nltk


class AnalysisException(Exception):
    pass


class OutMixin(object):
    def collect(self, item):
        raise NotImplementedError


class CorpusDBOut(OutMixin):
    def __init__(self):
        self.client = mongodb.Mongodb()

    def collect(self, item):
        desc = item.xpath("//jobinfo/text()")[0].encode("utf-8")
        cuts = jieba.cut(desc, cut_all=False)
        self.updateJobTagCorpus(cuts)

    def updateJobTagCorpus(self, cuts):
        collection = self.client.project_db["tagCorpus"]
        cuts_processed = set(cut.encode("utf-8") for cut in cuts if cut not in ["\n", "	"] and not cut.isdigit() and cut)
        collection.insert({"cuts": list(cuts_processed)})

    def buildIDF(self):
        try:
            tag_corpus = self.client.project_db["tagCorpus"]
            tag_IDF = self.client.project_db["tagIDF"]
            all_docs = tag_corpus.find()
            total_count = float(all_docs.count())
            for doc in all_docs:
                for cut in doc["cuts"]:
                    exist = tag_IDF.find_one_and_update({"word": cut},
                                                        {"$inc": {"count": 1}},
                                                        upsert=True)
            for IDF in tag_IDF.find():
                tag_IDF.find_one_and_update({"_id": IDF["_id"]},
                                            {"$set": {"IDF": math.log(total_count / (IDF["count"] + 1))}})
            return True
        except:
            print traceback.print_exc()
            return False


class Analysis(object):
    def __init__(self):
        self._finished = False

    def finish(self):
        self._finished = True

    def has_finished(self):
        return self._finished

    def get_results(self):
        raise NotImplementedError


class AddableDict(dict):
    def __add__(self, other):
        tmp = AddableDict(self)
        if isinstance(other, AddableDict):
            for k, v in other.items():
                if k in self:
                    tmp[k] += v
                else:
                    tmp[k] = v
            return tmp
        else:
            raise Exception("Not the same type!")

    def count(self, item):
        if item in self:
            self[item] += 1
        else:
            self.setdefault(item, 1)


class BasicAnalysis(Analysis, OutMixin):
    def __init__(self):
        super(BasicAnalysis, self).__init__()
        self._categories = AddableDict()

    def collect(self, xml):
        cats = xml.xpath("//indtype1/text()")
        cats.extend(xml.xpath("//indtype2/text()"))
        for c in cats:
            c = c.encode("utf-8")
            if c in self._categories:
                self._categories[c] += 1
            else:
                self._categories.setdefault(c, 1)

    def sum_up(self):
        total_collect = 0
        for value in self._categories.values():
            total_collect += value
        self._categories["total"] = total_collect

    def get_results(self, ajax=False):
        if ajax == "true":
            return self.get_results_for_echarts()
        else:
            self.sum_up()
            return self._categories

    def get_results_for_echarts(self):
        self.sum_up()
        results = []
        threshold = 0.015
        amount = 0
        for k, v in self._categories.iteritems():
            if k == "total":
                continue
            percentage = round(float(v) / self._categories["total"], 3)
            if percentage > threshold:  # 砍掉过少的部分优化展示
                results.append({"name": k, "value": percentage})
            else:
                amount += percentage
        results.append({"name": "其他", "value": round(amount, 3)})
        return results

    def combine(self, other):
        if isinstance(other, BasicAnalysis):
            self._categories += other._categories
        else:
            raise AnalysisException("Not the same type!")


class CommonTagsAnalysis(Analysis, OutMixin):
    """
    每条记录读取tfidf前排的标签，记录数据集合中标签数目，最后从所有的
    """
    def __init__(self):
        super(CommonTagsAnalysis, self).__init__()
        self._tags = AddableDict()
        self.allowPOS = ('ns', 'n', 'vn', 'ni', 'nx', 'nz', 'ng', 'b', 't', 'eng', 'l')

    def collect(self, xml):
        desc = xml.xpath("//jobinfo/text()")[0]
        tags = jieba_analyse.extract_tags(desc, topK=30, allowPOS=self.allowPOS, withWeight=True)
        for tag, weight in tags:
            self._tags[tag] = self._tags.setdefault(tag, 0) + weight

    def rank(self):
        tmp = sorted(list(item for item in self._tags.iteritems() if (not item[0].isdigit()) and item[0]), key=lambda x: x[1], reverse=True)
        self._tags = dict(tmp[:25])

    def get_results(self, ajax=False):
        self.rank()
        if ajax == "true":
            return self.get_results_for_echarts()
        else:
            print time.ctime()
            return self._tags

    def get_results_for_echarts(self):
        return [{"name": k, "value": v} for k, v in self._tags.iteritems()]


class IndustryTagsAnalysis(CommonTagsAnalysis):
    """
    方法同父类相同，只是字典从数据库中获得
    """
    def __init__(self):
        super(IndustryTagsAnalysis, self).__init__()
        self.client = mongodb.Mongodb()
        self.tfidf = my_tfidf.MyTFIDF(self.client.getJobIDF())

    def collect(self, xml):
        desc = xml.xpath("//jobinfo/text()")[0]
        tags = self.tfidf.extract_tags(desc, topK=30, allowPOS=self.allowPOS, withWeight=True)
        for tag, weight in tags:
            self._tags[tag] = self._tags.setdefault(tag, 0) + weight
            print tag, weight