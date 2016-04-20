edges = sc.textFile("graph5.txt")
pairs = edges.flatMap(lambda l: l.split()).map(lambda x:(x,1))
counts = pairs.reduceByKey(lambda a, b: a + b)
isEven = counts.map(lambda x:(None,(x[1])%2==0))
isEuler = isEven.reduceByKey(lambda a, b: False not in (a,b))
output = isEuler.collect()
print output[0][1]
