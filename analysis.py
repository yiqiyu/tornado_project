#!/usr/bin/env python
# -*- coding:utf-8 -*-
from common import mongodb
import time
from operator import itemgetter

import jieba
import jieba.analyse as jieba_analyse
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
        self.client.updateJobTagCorpus(cuts)



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
    def __init__(self):
        super(CommonTagsAnalysis, self).__init__()
        self._tags = AddableDict()

    def collect(self, xml):
        desc = xml.xpath("//jobinfo/text()")[0]
        tags = jieba_analyse.extract_tags(desc, topK=30)
        for tag in tags:
            self._tags.count(tag)

    def rank(self):
        tmp = sorted(list(self._tags.iteritems()), key=lambda x: x[1])
        self._tags = dict(tmp[:20])

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
    def __init__(self):
        super(IndustryTagsAnalysis, self).__init__()
        self.client = mongodb.Mongodb()
        self.tfidf = jieba_analyse.TFIDF(self.client.getJobIDF())

    def collect(self, xml):     #TODO
        pass