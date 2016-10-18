# email: ringzero@0x557.org
"""this is frequency example from mapreduce"""

def mapper(doc):
    # input reader and map function are combined
    import os
    words = []
    with open(os.path.join('./', doc)) as fd:
        for line in fd:
            for word in line.split():
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

word_count = {}
for f in ['doc1', 'doc2', 'doc3', 'doc4', 'doc5']:
    words = mapper(f)
    for word, count in reducer(words).iteritems():
        if word not in word_count:
            word_count[word] = 0
        word_count[word] += count

# sort words by frequency and print
for word in sorted(word_count, key=lambda x: word_count[x], reverse=True):
    count = word_count[word]
    print(word, count)

