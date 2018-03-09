"""
Utilities for working with Mozilla's Unified Telemetry data and standard derived
datasets.
"""

import mozillametricstools.common.functions as mmt
import mozillametricstools.common.s3 as s3fun


#-----------------------------------------------------------------------------
#
# Spark SQL shortcuts and patterns.


## SQL WHERE clause to identify rows corresponding to all valid Firefox profiles
## in main_summary.
MAIN_SUMMARY_FIREFOX = \
"""
vendor = 'Mozilla' AND
app_name = 'Firefox' AND
client_id is not null
"""

def iso_date_expr(col):
    """ SQL expression to convert a date column from yyyymmdd to ISO. """
    return """
            concat_ws('-',
                substring({date_col}, 1, 4),
                substring({date_col}, 5, 2),
                substring({date_col}, 7, 2)
            )
        """.format(date_col=col)


def pcd_expr(col):
    """ SQL expression to convert PCD from time since epoch to ISO date. """
    return "from_unixtime({pcd_col} * 24 * 3600, 'yyyy-MM-dd')"\
        .format(pcd_col=col)


#-----------------------------------------------------------------------------
#
# Derived datasets/DataFrames.


def filter_df_firefox_base(DF):
    """ Filter a dataset like main_summary for base valid Firefox profiles.

        Base valid profiles have 'app_name' set to "Firefox" and a non-null
        'client_id'.

        This expects flat fields and standard naming, like main_summary has,
        for 'client_id'/'app_name'. Checks 'vendor' field if present.

        Returns the given DataFrame with the filter applied.
    """
    DFcols = DF.columns
    if "client_id" not in DFcols or "app_name" not in DFcols:
        raise ValueError("The given DataFrame is missing at least one of " +
                         "'client_id' or 'app_name' columns")
    filter_expr = "client_id IS NOT NULL AND app_name = 'Firefox'"
    if "vendor" in DFcols:
        filter_expr += " AND vendor = 'Mozilla'"
    return DF.filter(filter_expr)


def load_parquet_dataset(spark, dataset_s3_path,
                            partition_path_components=None, base_filter=False):
    """ Load a Parquet dataset to a Spark DataFrame.

        spark: the current SparkSession
        dataset_s3_path: the full base path to the dataset in S3 (including
                         version number, if applicable).
        partition_path_components: to pre-filter to specific values of partition
                                   columns, specify a list of the corresponding
                                   partition path components (excluding the
                                   dataset base path). These should generally be
                                   generated using s3fun.partition_key_paths().
        base_filter: should the base filter be applied to restrict to valid
                     Firefox profiles?

        Returns a Spark DataFrame.
    """
    DF = spark.read.option("mergeSchema", "true")
    if partition_path_components:
        DF = DF.option("basePath", dataset_s3_path)\
            .parquet(*partition_path_components)
    else:
        DF = DF.parquet(dataset_s3_path)
    if base_filter:
        DF = filter_df_firefox_base(DF)
    return DF


def load_main_summary(spark, paths=None):
    """ Load main_summary from Parquet to a Spark DataFrame.

        Filters out records that are not from valid Firefox profiles.
        Optionally restricts to specific partition paths specified as a list.

        spark: the current SparkSession
        paths: S3 keys representing specific partition column values

        Returns a Spark DataFrame.
    """
    return load_parquet_dataset(spark,
                                s3fun.main_summary_path(),
                                paths,
                                base_filter=True)


def main_summary_partition_paths(start_date=None, end_date=None,
                                 start_sample_id=None, end_sample_id=None,
                                 dates=None, sample_ids=None):
    """ Find main_summary Parquet paths for the given submission dates and
        sample IDs.

        Dates and sample IDs can be specified either as an inclusive range with
        given start and end values, or as an explicit list of values. If a
        range is given with only one endpoint, the range will extend to the
        opposite valid min or max value. However, a date range must include a
        start date. If no sample IDs are specified, the paths do not include
        a sample ID (ie. they cover all samples).

        Dates should be strings in the "submission date" yyyymmdd format, and
        sample IDs are numbers between 0 and 99.

        Returns a list of paths corresponding to the subset of main_summary
        covering the combination (cartesian product) of all valid requested
        dates and sample IDs.
    """
    if not dates:
        if not start_date:
            raise ValueError("Dates must be given either as a list or a range" +
                " that includes a start date.")
        ## Convert date range endpoints to ISO for passing into date range
        ## function.
        start_date = mmt.submission_date_to_iso(str(start_date))
        end_date = mmt.today() if not end_date else \
            mmt.submission_date_to_iso(str(end_date))
        dmin = min(start_date, end_date)
        dmax = max(start_date, end_date)
        dates = mmt.dateRangeFromTo(dmin, dmax, out_fmt=mmt.SUBMISSION_DATE_FMT)
        ## The sequence generated by dateRangeFromTo() does not include the
        ## endpoint. Add it in to make the range inclusive.
        dates.append(mmt.iso_to_submission_format(dmax))
    else:
        if (start_date or end_date):
            raise ValueError("Dates must be given either as a list or a range.")
        ## Convert a single value to a list.
        if isinstance(dates, basestring):
            dates = [dates]

    if sample_ids is None:
        if (start_sample_id is not None or end_sample_id is not None):
            ## Expect a range of values between 0 and 99.
            start_sample_id = 0 if start_sample_id is None else \
                int(start_sample_id)
            end_sample_id = 99 if end_sample_id is None else int(end_sample_id)
            smin = min(start_sample_id, end_sample_id)
            smax = max(start_sample_id, end_sample_id)
            sample_ids = range(max(smin, 0), min(smax, 99) + 1)
    else:
        if (start_sample_id is not None or end_sample_id is not None):
            raise ValueError("Sample IDs must be given either as a list or a" +
                             " range.")
        ## Convert a single value to a list.
        if isinstance(sample_ids, (int, long, basestring)):
            sample_ids = [sample_ids]

    partition_cols = ["submission_date_s3"]
    partition_vals = [dates]
    if sample_ids is not None:
        partition_cols.append("sample_id")
        partition_vals.append(sample_ids)
    return s3fun.partition_key_paths(s3fun.main_summary_path(),
                                     partition_cols,
                                     partition_vals,
                                     check_valid=True)


#-----------------------------------------------------------------------------
#
# Raw ping data.


def parse_search_count_key(sc_key):
    """ Parse out search engine and SAP from a search count key.

        The search count key is of the form `<engine>.<sap>`.

        Returns None if the key was invalid, otherwise a dict with keys
        ["engine", "sap"].
    """
    ## Split the source and engine from the key.
    if "." not in sc_key:
        ## Shouldn't happen.
        return None

    ## The search engine identifier may itself contain "."s.
    ## However, the SAP shouldn't.
    engine, sap = sc_key.rsplit(".", 1)
    return {
        "engine": engine,
        "sap": sap
    }


def format_search_count_entry(sc_key, sc_val):
    """ Parse out search engine, SAP, count from a single keyed entry
        in the SEARCH_COUNTS histogram.

        - `sc_key`: the keyedHistogram key (of the form `<engine>.<sap>`).
        - `sc_val`: the corresponding count histogram.

        Returns a dict with keys ["engine", "sap", "count"], or None if the
        key was invalid.
    """
    parsed_sc = parse_search_count_key(sc_key)
    if not parsed_sc:
        return None
    ## Number of searches is the histogram total.
    parsed_sc["count"] = sc_val.get("sum", 0)
    return parsed_sc


