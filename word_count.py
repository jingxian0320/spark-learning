# count the number of distinct words in a txt file

import re
WORD_RE = re.compile(r"\b[a-z]+\b")
lines = sc.textFile("Lorem ipsum")
pairs = lines.flatMap(lambda l: WORD_RE.findall(l.lower())).map(lambda x:(x,1))
counts = pairs.reduceByKey(lambda a, b: a + b)
output = counts.sortByKey().collect()
for (word,count) in output:
    print("%s: %i" % (word, count))
