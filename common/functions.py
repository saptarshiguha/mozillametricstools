import datetime, time
import pyspark
import s3
import re


def latest_longitudinal_path():
    longit_versions = s3.list_subkeys(s3.S3_PARQUET_BUCKET,
                                      prefix="longitudinal/",
                                      last_component_only=True,
                                      include_contents=False)
    patt = re.compile(r"v\d{8}")
    longit_vers_validated = [v if patt.match(v) else "00000000"
                                 for v in longit_versions]
    max_version = max(longit_vers_validated)
    return (s3.S3_LONGITUDINAL_BASE_PATH + max_version, longit_vers_validated)




def dateRangeFromTo(start, stop=None, by=None, length=None):
    start1 = datetime.datetime.fromtimestamp(time.mktime( time.strptime(start,"%Y-%m-%d")))
    container = []
    if by:
        by = by
    else:
        by = 1
    if stop:
        stop1 = datetime.datetime.fromtimestamp(time.mktime( time.strptime(stop,"%Y-%m-%d")))
        x = start1
        while x < stop1:
            container.append( x.strftime("%Y-%m-%d"))
            x = x + datetime.timedelta(days=by)
    elif length:
        for x in range(0, length):
            container.append(( start1+datetime.timedelta(days = x*by)).strftime("%Y-%m-%d"))
    return container


def register_udf(sqlc,func, name, return_type):
    """ Register a UDF both for as a DataFrame Column and for use in Spark SQL.
        The Column udf will be assigned to the given name in the global scope,
        as well as registered with the SQLContext. Specify the UDF's return
        type as Spark type.
    """
    sqlc.registerFunction(name, func, return_type)
    globals()[name] = pyspark.sql.functions.udf(func, return_type)
