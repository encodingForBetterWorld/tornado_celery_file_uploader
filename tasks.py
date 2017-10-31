#!/usr/bin/env python
# encoding: utf-8

from celery import Celery
import time
import os

UPLOAD_PATH = os.path.join(os.path.dirname(__file__), 'files')  # 文件的暂存路径

if not os.path.exists(UPLOAD_PATH):
    os.mkdir(UPLOAD_PATH)

celery = Celery("tasks", broker="amqp://guest:guest@localhost:5672")
celery.conf.CELERY_RESULT_BACKEND = "amqp"
celery.conf.CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']


CHUNK_SIZE = 5 * 1024 * 1024

@celery.task
def sleep(seconds):
    time.sleep(float(seconds))
    return seconds

@celery.task
def upload_file(filename, data):
    ret = '0'
    filepath = os.path.join(UPLOAD_PATH, filename)
    try:
        with open(filepath, 'wb') as up:
            try:
                up.write(data)
            except Exception, e:
                print e.message
                ret = '105'
            finally:
                up.close()
    except Exception, e:
        print e.message
        ret = '102'
    finally:
        return ret

@celery.task
def upload_file_chunk(filename, chunk, idx, chunk_sum):
        ret = '0'
        file_path = os.path.join(UPLOAD_PATH, filename)
        is_exist = os.path.exists(file_path)
        w_mode = "rb+"
        if not is_exist:
            w_mode = "wb"
        try:
            with open(file_path, w_mode) as up:
                try:
                    w_idx = CHUNK_SIZE * idx
                    up.seek(w_idx)
                    up.write(chunk)
                except Exception, e:
                    print e.message
                    ret = '105'
                finally:
                    up.close()
        except Exception, e:
            print e.message
            ret = '102'
        finally:
            print "upload %s chunk idx %s" % (filename, idx)
            return ret


if __name__ == "__main__":
    celery.start()