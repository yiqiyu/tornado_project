#!/usr/bin/env python
# -*- coding:utf-8 -*-


class BasicAnalysis(object):
    def __init__(self):
        self.categories = {}

    def execute(self, xml):
        cats = xml.xpath("//indtype1/text()")
        cats.extend(xml.xpath("//indtype2/text()"))
        for c in cats:
            c = c.encode("utf-8")
            if c in cat:
                self.categories[c] += 1
            else:
                self.categories.setdefault(c, 0)