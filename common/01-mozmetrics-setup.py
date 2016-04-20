import os, sys, inspect
sys.path.insert(0, "/home/hadoop/")
from pyspark.sql import SQLContext
from pyspark.sql.types import *

bucket = "telemetry-parquet"
prefix = "main_summary/v2"
mainpingspq = sqlContext.read.load("s3://{}/{}".format(bucket, prefix), "parquet")

import mozillametricstools as mmt
