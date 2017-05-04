
"""
Utilities for interacting with Mozilla's data S3 buckets.
"""

import boto3

S3_PARQUET_BUCKET = "telemetry-parquet"

S3_METRICS_HOME_PATH = "s3://mozilla-metrics/user/"
S3_PARQUET_PATH = "s3://{}/".format(S3_PARQUET_BUCKET)
S3_MAIN_SUMMARY_BASE_PATH = S3_PARQUET_PATH + "main_summary/"
S3_LONGITUDINAL_BASE_PATH = S3_PARQUET_PATH + "longitudinal/"

## Access to high-level interface.
s3_resource = boto3.resource("s3")
## Access to underlying lower-level client interface.
## Operations like listing buckets are easier using this.
s3_client = s3_resource.meta.client


def main_summary_path():
    """ Returns the S3 path to the latest version `main_summary` dataset. """
    ms_versions = list_subkeys(S3_PARQUET_BUCKET,
                               prefix="main_summary/",
                               last_component_only=True,
                               include_contents=False)
    ## main_summary version path components are of the form
    ## "v1", "v2", "v3",...
    ms_versions.sort(reverse=True)
    latest_ms_version = ms_versions[0]
    return S3_MAIN_SUMMARY_BASE_PATH + latest_ms_version + "/"


def longitudinal_path(num_versions_before_latest=0):
    """ Returns the S3 path to a recent `longitudinal` dataset.

        If 'num_versions_before_latest' is positive, the returned path will
        correspond to that many versions prior to the latest. This functionality
        is useful when the latest dataset has some error, easily stepping back
        to a previous one.

        By default, the latest dataset is returned.
    """
    longit_versions = list_subkeys(S3_PARQUET_BUCKET,
                                 prefix="longitudinal/",
                                 last_component_only=True,
                                 include_contents=False)
    ## longitudinal version path components are of the form
    ## "v20170422", "v20170429",...
    longit_versions.sort(reverse=True)
    num_steps_back = 0
    if num_versions_before_latest:
        try:
            num_steps_back = int(num_versions_before_latest)
        except ValueError:
            print("Invalid value for number of versions before latest. " +
                  "Returning path to latest dataset.")
    latest_longit_version = longit_versions[num_steps_back]
    return S3_LONGITUDINAL_BASE_PATH + latest_longit_version + "/"


def list_subkeys(bucket_name, prefix="", last_component_only=True,
                                                    include_contents=False):
    """ List the next-level subkeys (common prefixes) of a bucket, starting
        from the given prefix.
    
        Returns a list of key prefixes up to (but not including) the next
        slash after the given prefix.  This is similar to how `aws s3 ls`
        works.
        
        - If the given prefix ends in "/", the returned key prefixes will
          include the next path component down from the given prefix, if any.
        - If the given prefix is non-empty but does not end in "/", the
          returned key prefixes complete the given prefix up to the end its
          last path component.
        - If the given prefix is empty, returns the top-level key prefixes in
          the bucket.
        
        If `last_component_only` is True, only the last path component (from
        the last "/" onwards) of the key prefixes will be returned.
        
        If `include_contents` is True, subkeys which are in fact full keys
        (have no further path components) will also be included.
        
        For example:
        
        - `list_subkeys("telemetry-parquet", prefix="crash")` returns
          `['crash_aggregates', 'crash_summary']`.
        - `list_subkeys("telemetry-parquet", prefix="main_summary/")` returns
          `['v1', 'v2', 'v3']`.
        - `list_subkeys("telemetry-parquet", prefix="main_summary/",
          strip_prefix=False)` returns `['main_summary/v1', 'main_summary/v2',
          'main_summary/v3']`.
    """
    
    bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name,
                                               Delimiter="/",
                                               Prefix=prefix)
    if bucket_objects.get("IsTruncated", False):
        ## There were more than 1000 (current ListObjects limit) keys.
        ## For now, just fail.
        raise NotImplementedError("Too many keys: " +
            "the returned key list was truncated. " +
            "Currently cannot handle this case.")
    
    ## Key prefixes, including the given prefix and up until the next "/"
    ## are returned in the "CommonPrefixes" object of the response.
    common_prefixes = [p["Prefix"]
                          for p in bucket_objects.get("CommonPrefixes", {})]
    ## Drop trailing slashes (all prefixes should have one).
    ## Remove the empty prefix, if present.
    key_prefixes = []
    for cp in common_prefixes:
        cp_no_slash = cp.rstrip("/")
        if cp_no_slash:
            key_prefixes.append(cp_no_slash)
    
    ## If we should include full keys (contents), add them from the
    ## "Contents" object.
    if include_contents:
        contents = [k["Key"] for k in bucket_objects.get("Contents", {})]
        key_prefixes = key_prefixes + contents
    
    ## Strip away higher path components, if required.
    if last_component_only and len(prefix) > 0:
        key_prefixes = [p.rsplit("/", 1)[-1] for p in key_prefixes]
    
    return key_prefixes

