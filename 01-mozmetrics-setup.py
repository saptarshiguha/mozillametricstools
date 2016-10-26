import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="myAppName")
sqlContext = pyspark.sql.SQLContext(sc)

import os, sys, inspect
sys.path.insert(0, "/home/hadoop/")
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as sqlfunctions



bucket = "telemetry-parquet"
prefix = "main_summary/v3"
mainpingspq = sqlContext.read.load("s3://{}/{}".format(bucket, prefix), "parquet")

print("-------------------------------------------------------------------------------")
print("1. main_summary is available as the DataFrame caled mainpingspq")
print("2. Save your python objects using saveObject(object), load in r via aswsobj")
print("3. import mozillametricstools !")
print("\nView more at: https://github.com/saptarshiguha/mozillametricstools")
print("---------------------------------------------------------------------------------")


def saveObject(s):
    import json
    with open('/tmp/pyobject.json', 'w') as outfile:
        json.dump(s, outfile)
        


