def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

lines = sc.textFile('graph5.txt')
links = lines.map(lambda l:(l.split()[0],[l.split()[1],]))
links = links.reduceByKey(lambda a,b: a+b).sortByKey(1)
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
for i in range (0,1):
    contribs = links.join(ranks).flatMap(
        lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(lambda a,b:a+b).mapValues(lambda rank: rank * 0.85 + 0.15)
ranks.collect()
