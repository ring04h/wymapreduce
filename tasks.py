#!/usr/bin/env python
# encoding: utf8
# email: ringzero@0x557.org

import time
from redis import Redis
from rq import Queue
from Queue import Queue as jobQueue
from utils import read, mapper, reducer

redis_conn = Redis('localhost', 6379)

mapper_q = Queue(connection=redis_conn)
reducer_q = Queue('high', connection=redis_conn)

mapper_Q = jobQueue()
reducer_Q = jobQueue()

def run():

    word_count = {}

    for emlid in range(0, 22455):
        mapper_job = mapper_q.enqueue(mapper, emlid, result_ttl=6000)
        mapper_Q.put(mapper_job)

    while not mapper_Q.empty():
        job = mapper_Q.get_nowait()

        while not job.is_finished:
            if job.is_failed:
                break
            time.sleep(0.5)

        if job.result is not None:
            # option: depends_on = mapper_job
            reducer_job = reducer_q.enqueue(reducer, job.result, result_ttl=6000)
            reducer_Q.put(reducer_job)

    while not reducer_Q.empty():

        job = reducer_Q.get_nowait()
        while not job.is_finished:
            if job.is_failed:
                break
            time.sleep(0.5)

        if job.result is not None:
            for word, count in job.result.iteritems():
                if word not in word_count:
                    word_count[word] = 0
                word_count[word] += count

    # sort words by frequency and print
    for word in sorted(word_count, key=lambda x: word_count[x], reverse=True):
        count = word_count[word]
        print(word, count)

if __name__ == '__main__':
    run()


