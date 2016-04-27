import os, sys, inspect
sys.path.insert(0, "/home/hadoop/")
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as sqlfunctions

bucket = "telemetry-parquet"
prefix = "main_summary/v2"
mainpingspq = sqlContext.read.load("s3://{}/{}".format(bucket, prefix), "parquet")
print("main_summary is available as the DataFrame caled mainpingspq")
print("Save your python objects using saveObject(object)")


def saveObject(s):
    import json
    with open('/tmp/pyobject.json', 'w') as outfile:
        json.dump(s, outfile)
        

import mozillametricstools as mmt
