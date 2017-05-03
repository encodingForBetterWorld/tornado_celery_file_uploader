# coding=utf-8
#!/usr/bin/env python
# encoding: utf-8

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen
import tornado.httpclient
import tcelery, tasks
import os
from tornado.options import define, options

define("port", default=8000, help="run on the given port", type=int)

tcelery.setup_nonblocking_producer()

class SleepHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        # tornado.gen.Task的参数是:要执行的函数, 参数
        response = yield tornado.gen.Task(tasks.sleep.apply_async, args=[3])
        self.write("when i sleep %d s" % response.result)
        self.finish()


class UploadFileHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("upload_file.html")


    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        req = self.request
        file_metas = req.files['file']  # 提取表单中‘name’为‘file’的文件元数据
        chunk_idx, chunks_sum = None, None
        chunk = req.arguments.get('chunk')
        if chunk is not None:
            chunk_idx = int(chunk[0])
        chunks = req.arguments.get('chunks')
        if chunks is not None:
            chunks_sum = int(chunks[0])
        upload_path = os.path.join(os.path.dirname(__file__), 'files')  # 文件的暂存路径
        ret = 0
        for meta in file_metas:
            filename = meta['filename']
            data = meta['body']
            filepath = os.path.join(upload_path, filename)
            if chunk_idx is None and chunks_sum is None:
                response = yield tornado.gen.Task(tasks.upload_file.apply_async, args=[filepath, data])
            else:
                response = yield tornado.gen.Task(tasks.upload_file_chunk.apply_async, args=[filepath, data, chunk_idx, chunks_sum])
            ret = response.result
        if ret == '0':
            self.finish('{"jsonrpc" : "2.0", "result" : completed, "id" : "id"}')
        elif ret == '102':
            self.finish('{"jsonrpc" : "2.0", "error" : {"code": 102, "message": "Failed to open output stream."}, "id" : "id"}')
        elif ret == '105':
            self.finish('{"jsonrpc" : "2.0", "error" : {"code": 105, "message": "error occur when writing."}, "id" : "id"}')

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/sleep", SleepHandler),
            (r"/file_upload", UploadFileHandler),
        ]
        settings = {
            'template_path': 'templates',
            'static_path': 'static'
        }
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = Application()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()