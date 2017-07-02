#!/usr/bin/env python
# -*- coding:utf-8 -*-
from jieba.analyse.tfidf import DEFAULT_IDF, IDFLoader
import jieba.posseg
import jieba.analyse as jieba_analyse
import StringIO

class MyIDFLoader(IDFLoader):

    def __init__(self, idf_path_or_io=None):
        super(MyIDFLoader,self).__init__(idf_path_or_io)

    def set_new_path(self, new_idf_path_or_io):
        if self.path != new_idf_path_or_io:
            self.path = new_idf_path_or_io
            if isinstance(new_idf_path_or_io, str):
                content = open(new_idf_path_or_io, 'rb').read().decode('utf-8')
            elif isinstance(new_idf_path_or_io, StringIO.StringIO):
                content = new_idf_path_or_io.getvalue()             #本地测试需要解码utf-8
            else:
                content = ""
            self.idf_freq = {}
            for line in content.splitlines():
                word, freq = line.strip().split(' ')
                self.idf_freq[word] = float(freq)
            self.median_idf = sorted(
                self.idf_freq.values())[len(self.idf_freq) // 2]


class MyTFIDF(jieba_analyse.TFIDF):

    def __init__(self, idf_path=None):
        self.tokenizer = jieba.dt
        self.postokenizer = jieba.posseg.dt
        self.stop_words = self.STOP_WORDS.copy()
        self.idf_loader = MyIDFLoader(idf_path or DEFAULT_IDF)
        self.idf_freq, self.median_idf = self.idf_loader.get_idf()