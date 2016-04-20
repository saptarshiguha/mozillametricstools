def saveObject(s):
    import json
    with open('/tmp/pyobject.json', 'w') as outfile:
        json.dump(s, outfile)
        

def computeCountsOfVar(df,whatvar):
    x = df.groupBy(whatvar).count().collect()
    x = [x.asDict() for x in x]
    x = sorted(x, key=lambda s: -s['count'])
    return(x)


## Example Usage
# mpqs = mainpingspq.sample(False,0.001)
# mpqs = mpqs.cache()

# gr2=mpqs.groupby("client_id").agg({"client_id": 'count'}).select('client_id',col('count(client_id)').alias('pinglen'))
# h=computeCountsOfVar( gr1, "pinglen")



