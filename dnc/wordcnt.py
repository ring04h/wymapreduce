#!/usr/bin/env python
# encoding: utf8

import os
from flanker import mime
from flanker.addresslib import address

def read(fileName):
    result = ''
    filepath = os.path.join('./emls/', fileName)
    with open(filepath, 'r') as fd:
        for line in fd.readlines():
            result += line
    return result

def mapper(data):
    words = []
    for word in data.split():
        if len(word) > 3 and word.isalpha():
            words.append((word.lower(), 1))
    return words

def reducer(words):
    # we should generate sorted lists which are then merged,
    # but to keep things simple, we use dicts
    word_count = {}
    for word, count in words:
        if word not in word_count:
            word_count[word] = 0
        word_count[word] += count
    # print('reducer: %s to %s' % (len(words), len(word_count)))
    return word_count

if __name__ == '__main__':

    word_count = {}

    for i in range(1,22455):
        filename = '{}.eml'.format(i)
        mime_msg = read(filename)
        msg = mime.from_string(mime_msg)

        if msg.content_type.is_multipart():
            for part in msg.parts:
                if part.body is not None:
                    words = mapper(part.body)
                    for word, count in reducer(words).iteritems():
                        if word not in word_count:
                            word_count[word] = 0
                        word_count[word] += count
        else:
            if msg.body is not None:
                words = mapper(msg.body)
                for word, count in reducer(words).iteritems():
                    if word not in word_count:
                        word_count[word] = 0
                    word_count[word] += count

    # sort words by frequency and print
    for word in sorted(word_count, key=lambda x: word_count[x], reverse=True):
        count = word_count[word]
        print(word, count)


