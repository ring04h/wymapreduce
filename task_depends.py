#!/usr/bin/env python
# encoding: utf8
# email: ringzero@0x557.org

import time
from redis import Redis
from rq import Queue

from Queue import Queue as jobQueue
from utils import mapper, reducer

redis_conn = Redis('localhost', 6379)
q = Queue(connection=redis_conn)

# lower_bound, upper_bound = 1000, 2000
reducer_Q = jobQueue()

def run():

    word_count = {}

    for emlid in range(1, 1000):
        mapper_job = q.enqueue(mapper, emlid, result_ttl=36000)
        reducer_job = q.enqueue(reducer, depends_on=mapper_job, result_ttl=36000)
        reducer_Q.put(reducer_job)

        # if q.count >= upper_bound:
        #     while q.count > lower_bound:
        #         time.sleep(1)

    while not reducer_Q.empty():
        job = reducer_Q.get_nowait()
        while not job.is_finished:
            if job.is_failed: break
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


