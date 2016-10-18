#!/usr/bin/env python
# encoding: utf8
# email: ringzero@0x557.org

import os
from flanker import mime
from flanker.addresslib import address

from rq import get_current_job
from redis import Redis

redis_conn = Redis('localhost', 6379)

def read(fileName):
    result = ''
    filepath = os.path.join('./dnc/emls/', fileName)
    with open(filepath, 'r') as fd:
        for line in fd.readlines():
            result += line
    return result

def split_words(data):
    words = []
    for word in data.split():
        if len(word) > 3 and word.isalpha():
            words.append((word.lower(), 1))
    return words

def mapper(emailid):
    words = []

    filename = '{}.eml'.format(emailid)
    mime_msg = read(filename)
    msg = mime.from_string(mime_msg)

    if msg.content_type.is_multipart():
        for part in msg.parts:
            if part.body is not None:
                words.extend(split_words(part.body))
    else:
        if msg.body is not None:
            words.extend(split_words(msg.body))

    return words

def reducer():
    current_job = get_current_job(redis_conn)
    words = current_job.dependency.result
    
    # we should generate sorted lists which are then merged,
    # but to keep things simple, we use dicts
    word_count = {}
    for word, count in words:
        if word not in word_count:
            word_count[word] = 0
        word_count[word] += count
    # print('reducer: %s to %s' % (len(words), len(word_count)))
    return word_count

