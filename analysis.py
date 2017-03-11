#!/usr/bin/env python
# -*- coding:utf-8 -*-
import multiprocessing


class AnalysisException(Exception):
    pass


class OutMixin(object):
    def collect(self, item):
        raise Exception("Method collect hasn't been implemented.")


class OutQueue(OutMixin):
    def __init__(self):
        self.q = multiprocessing.Queue()

    def collect(self, item):
        self.q.put(item)


class Analysis(object):
    def __init__(self):
        self._finished = False

    def finish(self):
        self._finished = True

    def has_finished(self):
        return self._finished


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

    def get_results(self):
        self.sum_up()
        return self._categories

    def combine(self, other):
        if isinstance(other, BasicAnalysis):
            self._categories += other._categories
        else:
            raise AnalysisException("Not the same type!")

