import datetime
import pyspark
import s3
import re


ISO_DATE_FMT = "%Y-%m-%d"
SUBMISSION_DATE_FMT = "%Y%m%d"


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


def dateRangeFromTo(start, stop=None, by=1, length=None, out_fmt=ISO_DATE_FMT):
    """ Generate a sequence of dates, similarly to seq() in R.

        Either the end date or the desired length of the sequence should be
        specified (end date takes precedence if both are given).

        Start and stop dates are expected in ISO format.
        Returns the date sequence as a list of strings in the specified format
        (defaulting to ISO).
    """
    start1 = iso_to_date_obj(start)
    if not start1:
        raise ValueError("Invalid 'start' date.")

    container = []
    if stop:
        stop1 = iso_to_date_obj(stop)
        if not stop1:
            raise ValueError("Invalid 'stop' date.")

        x = start1
        while x < stop1:
            container.append(x)
            x += datetime.timedelta(days=by)
    elif length:
        container = [start1 + datetime.timedelta(days=x * by)
                         for x in range(length)]
    return [d.strftime(out_fmt) for d in container]


def register_udf(sqlc, func, name, return_type):
    """ Register a UDF both for as a DataFrame Column and for use in Spark SQL.

        The Column udf will be assigned to the given name in the global scope,
        as well as registered with the SQLContext (meaning that it can be used
        in Spark SQL snippets).

        sqlc: the current sqlContext
        func: the UDF to be registered, which will be applied to each element of
              a DF Column
        name: the name to be assigned
        return_type: the `pyspark.sql.types.DataType` returned by the UDF. This
                     needs to be specified to ensure schema consistency.
    """
    sqlc.registerFunction(name, func, return_type)
    globals()[name] = pyspark.sql.functions.udf(func, return_type)


#-----------------------------------------------------------------------------
#
# Date handling.

def submission_date_to_iso(submission_date):
    """ Convert a submission datestring of the form 'yyyymmdd' to an ISO format
        datestring.

        Returns None if there is a problem parsing the date.
    """
    if not submission_date:
        return None
    try:
        as_date = datetime.datetime.strptime(submission_date,
                                             SUBMISSION_DATE_FMT)
        return as_date.strftime(ISO_DATE_FMT)
    except ValueError:
        return None


def pcd_to_iso(pcd):
    """ Convert a profile creation date, supplied as a Unix datestamp, to an ISO
        format datestring.

        Returns None if the date is invalid: unparseable or unreasonably old.
    """
    if not pcd or pcd < 0:
        return None
    ## Convert the PCD to seconds since the epoch first.
    as_date = datetime.datetime.utcfromtimestamp(pcd * 86400)
    pcd_iso = as_date.strftime(ISO_DATE_FMT)
    if pcd_iso < "2000-01-01":
        return None
    return pcd_iso


def iso_to_submission_format(iso_date):
    """ Convert an ISO datestring back to the submission date format 'yyyymmdd'.

        If the input is missing/empty, returns None.
    """
    if not iso_date:
        return None
    return iso_date.replace("-", "")


def is_proper_iso_date(date_str):
    """ Check that a string intended to contain an ISO date is interpretable as
        a date.
    """
    return iso_to_date_obj(date_str) is not None


def iso_to_date_obj(date_str):
    """ Convert an ISO datestring into a datetime.date object.

        The datestring is checked for validity. A valid datestring starts with
        "yyyy-mm-dd", and optionally continues with more detailed time info.
        Everything but the initial date portion is ignored.

        Returns a datetime.date object representing the date portion of the
        string, or None if the datestring is invalid.
    """
    if not date_str or len(date_str) < 10:
        return None
    date_str = date_str[:10]
    try:
        return datetime.datetime.strptime(date_str, ISO_DATE_FMT)
    except ValueError:
        return None


def format_iso_date(date_str):
    """ Given an ISO datetime string, checks for validity and converts to a valid
        ISO date string.

        A valid datestring starts with "yyyy-mm-dd", and optionally continues
        with more detailed time info. Everything but the initial date portion is
        ignored.

        Returns a string of the form "yyyy-mm-dd" if the datestring is valid, or
        None otherwise.
    """
    as_date = iso_to_date_obj(date_str)
    if not as_date:
        return None
    return as_date.strftime(ISO_DATE_FMT)


def now():
    """ Human-readable string giving current date and time. """
    return datetime.datetime.now().ctime()


def today():
    """ ISO-formatted date string. """
    return datetime.date.today().isoformat()

