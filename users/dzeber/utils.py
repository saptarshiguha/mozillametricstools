
"""
Useful functions and utilities for working with Mozilla data in Spark.
"""

import sys
import pyspark.sql.functions as sparkfun
from pyspark.sql import Column as sparkCol
from pyspark.sql.types import StructType


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

## SQL WHERE clause to identify rows corresponding to all valid Firefox profiles
## in main_summary.
MAIN_SUMMARY_FIREFOX = """
    vendor = 'Mozilla' AND
    app_name = 'Firefox' AND
    client_id is not null
"""


def count_distinct(DF, cols):
    """ Count distinct values of the given column or combination of columns.

        Runs the job and returns an int.
    """
    return DF.select(*_col_arg_as_list(cols)).distinct().count()


def get_some_values(DF, cols, first_n=None, fraction=None, sample_n=None):
    """ Get some sample unique values of certain columns in a DataFrame.

        Exactly one of 'first_n', 'fraction', or 'sample_n' should be given.

        first_n: take this many first unique values (using `take()`).
        fraction: sample this fraction of the unique values.
        sample_n: draw a sample of unique values of this target size. First
                  runs a job to compute the total number of unique rows.

        Returns a list of values, if a single column is specified, otherwise
        a list of Rows.
    """
    n_args_supplied = 0
    for arg in [first_n, fraction, sample_n]:
        if arg:
            n_args_supplied += 1
    if n_args_supplied != 1:
        raise ValueError("Exactly one of `first_n`, `fraction`, or" +
            " `sample_n` should be specified.")
    cols = _col_arg_as_list(cols)
    DF_unique = DF.select(*cols).distinct()
    if first_n:
        some_rows_coll = DF_unique.take(first_n)
    else:
        if sample_n:
            n_tot = DF_unique.count()
            fraction = float(sample_n) / n_tot
        some_rows = DF_unique.sample(withReplacement=False, fraction=fraction)
        some_rows_coll = some_rows.collect()
    if len(cols) == 1:
        colname = DF_unique.columns[0]
        some_rows_coll = [r[colname] for r in some_rows_coll]
    return some_rows_coll


def show_df(DF, n_rows=10):
    """ Deprecated: this has been moved to display.py.

        Display the first few rows of the Spark DataFrame as a Pandas DataFrame.
    """
    return DF.limit(n_rows).toPandas()


def any_agg_fun(bool_col, agg_colname=None):
    """ Aggregate 'any()' function to be applied to a Boolean DataFrame Column.

        bool_col: the boolean column to be aggregated. If a string, it will be
                  interpreted as a column name.
        agg_colname: the name to use for the new aggregate column. If `bool_col`
                     is a string and `agg_colname` is None, `bool_col` will be
                     used. If `bool_col` is a Column, `agg_colname` must be
                     specified.
    """
    if isinstance(bool_col, basestring):
        agg_colname = agg_colname or bool_col
        bool_col = _as_col(bool_col)
    if not isinstance(bool_col, sparkCol):
        raise ValueError("The boolean column arg must be either a Column name" +
            " string or a Column.")
    if not agg_colname:
        raise ValueError("If the boolean column is not a column name string," +
                         " the aggregate column name must be specified.")
    num_true_vals = sparkfun.sum(bool_col.cast("int"))
    any_true_vals = num_true_vals > 0
    return any_true_vals.alias(agg_colname)


def replace_null_values(DF, cols, replacement):
    """ Replace null values in the specified columns with the given replacement
        value.

        cols: a column name string or list of column name strings
        replacement: a single string to replace each null value with
    """
    for colname in _col_arg_as_list(cols, string_only=True):
        col = _as_col(colname)
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


def build_schema_from_spec(schema_spec):
    """ Build a DataFrame schema from a simplified spec.

        The spec should be a list of elements of the form
        (<name>, <type>, <nullable?>). The <nullable> entry is
        optional and defaults to True. The list ordering is preserved
        as the final column ordering.

        For example:
        [("client_id", "string", False),
         ("channel", "string", False),
         ("os", "string"),
         ("total_time", "long")]

        Returns a StructType representing the schema.
    """
    def field_spec_to_dict(field_spec):
        fld_sch = {
            "name": field_spec[0],
            "type": field_spec[1],
            "nullable": True,
            "metadata": {}
        }
        if len(field_spec) > 2:
            fld_sch["nullable"] = field_spec[2]
        return fld_sch

    schema_flds = [field_spec_to_dict(s) for s in schema_spec]
    schema_dict = {
        "fields": schema_flds,
        "type": "struct"
    }
    return StructType.fromJson(schema_dict)


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


def get_col_name(col):
    """ Look up the name for the given Column. """
    if isinstance(col, basestring):
        return col
    if not isinstance(col, sparkCol):
        raise ValueError("Arg must be a Column object or string.")
    return col._jc.toString()


def _as_col(colname):
    """ Return a column object, converting from a name string if necessary.

        Get a Spark Column object from its name, if the arg is a string.
        If the arg is already a Column, just return it unchanged. Otherwise,
        raise an exception.
    """
    if isinstance(colname, sparkCol):
        return colname
    if isinstance(colname, basestring):
        return sparkfun.col("`{}`".format(colname))
    raise ValueError("Column arg must be either a string or a Column.")


def _col_arg_as_list(colarg, string_only=False, col_only=False):
    """ Convert a function arg representing 1 or more Columns to a list.

        If the arg is a string or a Column, insert it into a list.
        Otherwise, check that arg is a list or tuple, and check that its
        contents are either strings or Columns.

        string_only: accept only a single string or list-like of strings
        col_only: accept only a single Column or list-like of Columns

        At most one of these switches may be True.

        Returns a list of either Column name strings or Columns, depending
        on `string_only` or `col_only`.
    """
    if string_only and col_only:
        raise ValueError("At most one of 'string_only' and 'col_only' may" +
            " be true in _col_arg_as_list().")

    def col_type_descr(msg, plural=True):
        COL_TYPE_STR_DESCR = "column name string{pl}"
        COL_TYPE_OBJ_DESCR = "Column object{pl}"
        if string_only:
            col_type_descriptor = COL_TYPE_STR_DESCR
        else:
            if col_only:
                col_type_descriptor = COL_TYPE_OBJ_DESCR
            else:
                col_type_descriptor = "{} or {}".format(COL_TYPE_STR_DESCR,
                                                   COL_TYPE_OBJ_DESCR)
        plural_str = "s" if plural else ""
        col_type_descriptor = col_type_descriptor.format(pl=plural_str)
        return msg.format(col_type_descriptor)

    single_val_error = col_type_descr("Columns must be specified as {}.")
    if isinstance(colarg, basestring):
        if col_only:
            raise ValueError(single_val_error)
        else:
            return [colarg]
    if isinstance(colarg, sparkCol):
        if string_only:
            raise ValueError(single_val_error)
        else:
            return [colarg]

    list_error = col_type_descr(
        "Column arg must be either a {} or a list/tuple.",
        plural=False)
    if type(colarg) not in (list, tuple):
        raise ValueError(list_error)
    elt_error = col_type_descr("Column list elements must be {}.")
    for col in colarg:
        if (not (isinstance(col, basestring) or isinstance(col, sparkCol)) or
                (isinstance(col, basestring) and col_only) or
                (isinstance(col, sparkCol) and string_only)):
            raise ValueError(elt_error)
    return list(colarg)

