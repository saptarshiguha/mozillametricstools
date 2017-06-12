
"""
Useful functions and utilities for working with Mozilla data in Spark.
"""

import sys
import pyspark.sql.functions as sparkfun



#-----------------------------------------------------------------------------
#
# Telemetry data handling.


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


#-----------------------------------------------------------------------------
#
# Spark/DataFrame utils.


def show_df(DF, n_rows=10):
    """ Display the first few rows of the Spark DataFrame as a Pandas DataFrame.
    """
    return DF.limit(n_rows).toPandas()


def replace_null_values(DF, cols, replacement):
    """ Replace null values in the specified columns with the given replacement
        value.

        cols: a list of column names
        replacement: a single string to replace each null value with
    """
    for colname in cols:
        col = sparkfun.col("`{}`".format(colname))
        col_with_repl = sparkfun.when(col.isNull(), replacement).otherwise(col)
        DF = DF.withColumn(colname, col_with_repl)
    return DF


def null_to_zero(DF, cols):
    """ Convert null values in given (numeric) DataFrame columns to zeros.
    
        This is useful in creating pivot tables where some combinations don't
        occur. They get a `null` count by default rather than a count of 0.

        cols: a list of column names
    """
    return replace_null_values(DF, cols, replacement=0)


def write_schema_to_file(DF, filepath="schema.txt"):
    """ Write the schema of the specified DataFrame to a file.
    
        This is useful for DataFrames with many columns, like the longitudinal
        dataset, so as to be able to navigate the schema using paging and search.
    """
    orig_stdout = sys.stdout
    with open(filepath, "w") as f:
        sys.stdout = f
        DF.printSchema()
    sys.stdout = orig_stdout


