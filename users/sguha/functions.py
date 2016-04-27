
def computeCountsOfVar(df,whatvar):
    x = df.groupBy(whatvar).count().collect()
    x = [x.asDict() for x in x]
    x = sorted(x, key=lambda s: -s['count'])
    return(x)

def dateToUnixNanoSec(dt,format="%Y-%m-%d"):
    import time, datetime
    d = datetime.datetime.fromtimestamp(time.mktime( time.strptime(dt,format)))
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (d - epoch).total_seconds() * 1000000000
    
def UnixNanoSecToDT(s):
    from datetime import datetime
    dt = datetime.fromtimestamp(s // 1000000000)
    return (dt.strftime('%Y-%m-%d '),dt.strftime('%Y-%m-%d %H:%M:%S'))



# ## Example Usage
# mpqs = mainpingspq.sample(False,0.2)
# gr2 = mpqs.groupby("client_id").agg({"client_id": 'count'}).select(col('count(client_id)').alias('pinglen'))
# gr3 = gr2.groupBy("pinglen").count().collect()
# gr3 = [x.asDict() for x in gr3]
# gr3 = sorted(gr3, key=lambda s: -s['count'])

## i take this object into R and compute the weighted quantiles
## based on this any profile with 15,000 + pings is dropped
## that is (100 - 99.999 %) of profiles
## let's get this exclusion list

# from operator import add
# mpqs = mainpingspq
# gr2 = mpqs.groupby("client_id").agg({"client_id": 'count'}).select(col("client_id"),col('count(client_id)').alias('pinglen'))
# clientexclusion = gr2.filter(gr2.pinglen > 15000)
# clientexclusion.write.save("telemetry-test-bucket/sguhatmp/tmp/clientexclusion1.parquet")
# clxList = sqlContext.read.load("telemetry-test-bucket/sguhatmp/tmp/clientexclusion1.parquet").collect()
# clxList = [ x.client_id for x in clxList]

# 59 555 082 387
# from operator import add
# def combFunc(u,v):
#     u.append(v)
#     u = sorted(u, key=lambda g: g.subsession_start_date)
#     l = len(u)
#     if l < 5000:
#         t = l
#     else:
#         t = 5000
#     return  u[ -t:]

# def redFunc(u,v):
#     u = u + v
#     u = sorted(u, key=lambda g: g.subsession_start_date)
#     l = len(u)
#     if l < 5000:
#         t = l
#     else:
#         t = 5000
#     return  u[ -t:]

# ## 1. exclude the massive clients
# x = mpqs #.sample(False,0.0001).cache()
# ## u = x.rdd.take(1)
# t1  = x.rdd.filter(lambda s: s.client_id not  in clxList)
# ## 2. map to key, valye where key is the client_id
# t2 = t1.map(lambda d: (d.client_id,d))
# t3 = t2.aggregateByKey([], combFunc, redFunc)
# t3.saveAsSequenceFile("telemetry-test-bucket/sguhatmp/tmp/newformdata.sq")



# ss = sqlContext.read.load("telemetry-test-bucket/sguhatmp/plens12.parquet", "parquet")
# ss.count()


# h=computeCountsOfVar( gr2, "pinglen")

# 0.001, 377
# 0.1 ,5285
# 0.25, 9208
# 0.5, 14022
# 0.75, 18059
# 0.9, 20218
# 0.99, 21499
# 0.999, 21633
# 1, 21590
