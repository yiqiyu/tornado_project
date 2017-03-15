#!/usr/bin/env python
# -*- coding:utf-8 -*-

import socket
import os

import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define, options

import view

host_name = socket.gethostname()
server_info = {
    "status": 200,
    "host_name": host_name,
    "system_name": "tornado-API",
}
define("port", default=8888, type=int)
define("concurrent", default=0, type=int)


class APIHandler(tornado.web.RedirectHandler):
    def get(self):
        self.write(server_info)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [("/", view.MainHandler),
                    ("/api", APIHandler),
                    ("/api/search", view.CrawlerHandler),
                    ("/api/gev_search", view.GevCrawlerHandler),
                    ]
        option = {
            "debug": False,
            "template_path": "./template",
            "static_path": "./static"
                 }
        tornado.web.Application.__init__(self, handlers, **option)


def main():
    options.logging = "info"
    options.log_file_prefix = "logs/app.logs"
    options.log_file_max_size = 100 * 1024 * 1024
    options.log_file_num_backups = 3
    options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.bind(options.port)
    # http_server.bind(8000)
    http_server.start(options.concurrent)
    print "Online!"
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
