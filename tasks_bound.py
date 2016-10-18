#!/usr/bin/env python
# encoding: utf8
# email: ringzero@0x557.org

"""
rq.queue, redis store 100 tasks,
only allowed to run simultaneously 100 instances
lower_bound, upper_bound = 20, 100

process communication, share the Queue()
"""

import time
from redis import Redis
from rq import Queue
from Queue import Queue as jobQueue
from utils import read, mapper, reducer
from threading import Condition

redis_conn = Redis('localhost', 6379)

mapper_q = Queue(connection=redis_conn)
reducer_q = Queue('high', connection=redis_conn)

mapper_Q = jobQueue()
reducer_Q = jobQueue()

lower_bound, upper_bound = 100, 200

def run():

    word_count = {}

    for emlid in range(1, 22455):
        mapper_job = mapper_q.enqueue(mapper, emlid, result_ttl=36000)
        mapper_Q.put(mapper_job)

        # redis queue() size is over upper
        if mapper_q.count >= upper_bound:
            while mapper_q.count > lower_bound:
                # print "mapper_q is {}, reducer_q is {}".format(
                    # mapper_q.count, reducer_q.count)
                time.sleep(1)
            
            while reducer_q.count < lower_bound:
                if not mapper_Q.empty():
                    job = mapper_Q.get_nowait()

                    if job.is_finished:
                        if job.result is not None:
                            # option: depends_on = mapper_job
                            reducer_job = reducer_q.enqueue(reducer, job.result, result_ttl=36000)
                            reducer_Q.put(reducer_job)
                    elif job.is_failed:
                        pass
                    else:
                        mapper_Q.put(job)
                else:
                    break

    while not mapper_Q.empty():

        while reducer_q.count >= upper_bound:
            time.sleep(1)

        job = mapper_Q.get_nowait()
        while not job.is_finished:
            if job.is_failed:
                break
            time.sleep(1)

        if job.result is not None:
            reducer_job = reducer_q.enqueue(reducer, job.result, result_ttl=36000)
            reducer_Q.put(reducer_job)

    while not reducer_Q.empty():
        job = reducer_Q.get_nowait()
        while not job.is_finished:
            if job.is_failed:
                break
            time.sleep(1)

        if job.result is not None:
            for word, count in job.result.iteritems():
                if word not in word_count:
                    word_count[word] = 0
                word_count[word] += count

    for word in sorted(word_count, key=lambda x: word_count[x], reverse=True):
        count = word_count[word]
        print(word, count)

if __name__ == '__main__':
    run()



