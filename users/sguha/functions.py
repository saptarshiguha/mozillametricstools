from pyspark.sql import SQLContext
from pyspark.sql.types import *

bucket = "telemetry-parquet"
prefix = "main_summary/v2"
%time mainpingspq = sqlContext.read.load("s3://{}/{}".format(bucket, prefix), "parquet")



## Example Code
## 
def computeCountsOfVar(df,whatvar):
    x = df.agg(countDistinct(whatvar)).collect()
    return(x)


## Example
gr1 = mainpingspq.sample(False,0.01).groupby("client_id").agg({"client_id": 'count'})
gr1.cache()
h=computeCountsOfVar( gr1.select(col("count(client_id)").alias("pingLengthPerProfile")), "pingLengthPerProfile")

