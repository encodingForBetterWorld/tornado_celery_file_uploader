#!/usr/bin/env python
# encoding: utf-8

from celery import Celery
import time
import os
from record_redis import redis_client

celery = Celery("tasks", broker="amqp://guest:guest@localhost:5672")
celery.conf.CELERY_RESULT_BACKEND = "amqp"
celery.conf.CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']

# 存入redis中

@celery.task
def sleep(seconds):
    time.sleep(float(seconds))
    return seconds

@celery.task
def upload_file(filepath, data):
    ret = '0'
    try:
        with open(filepath, 'wb') as up:
            try:
                up.write(data)
            except Exception, e:
                print e.message
                redis_client.set(filepath, 'error')
                ret = '105'
            finally:
                up.close()
    except Exception, e:
        print e.message
        ret = '102'
    finally:
        return ret

@celery.task
def upload_file_chunk(filepath, chunk, idx, sum):
        ret = '0'
        is_exe = False
        if idx is 0:
            w_mode = 'wb'
        else:
            w_mode = 'ab'
        # 为了避免exe文件上传错误的BUG,去除exe文件后缀
        if filepath[-4:] == '.exe':
            is_exe = True
            filepath = filepath[:-4]
        # 如果分片顺序错误，阻塞当前worker
        while True:
            pre_idx = redis_client.get(filepath)
            # 顺序正确时解除阻塞
            if pre_idx is None:
                if idx == 0:
                 break
            elif pre_idx == 'error':
                ret = '105'
                if idx == sum - 1:
                    print 'upload %s fail' % filepath
                    redis_client.delete(filepath)
                return ret
            else:
                if int(pre_idx) == idx - 1:
                    break
            time.sleep(.1)
        try:
            with open(filepath, w_mode) as up:
                try:
                    up.write(chunk)
                except Exception, e:
                    print e.message
                    redis_client.set(filepath, 'error')
                    ret = '105'
                finally:
                    up.close()
        except Exception, e:
            print e.message
            ret = '102'
        finally:
            print "write %d : chunk size is %d" % (idx, len(chunk))
            if idx == sum - 1:
                print 'upload file to %s accomplish' % filepath
                if is_exe:
                    exe_name = filepath+'.exe'
                    if os.path.exists(exe_name):
                        os.remove(exe_name)
                    os.rename(filepath, exe_name)
                redis_client.delete(filepath)
            else:
                redis_client.set(filepath, idx)
            return ret


if __name__ == "__main__":
    celery.start()