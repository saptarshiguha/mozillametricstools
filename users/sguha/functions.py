


## Example Code
## 
def computeCountsOfVar(df,whatvar):
    x = df.agg(countDistinct(whatvar)).collect()
    return(x)


## Example
gr1 = mainpingspq.sample(False,0.01).groupby("client_id").agg({"client_id": 'count'})
gr1.cache()
h=computeCountsOfVar( gr1.select(col("count(client_id)").alias("pingLengthPerProfile")), "pingLengthPerProfile")

