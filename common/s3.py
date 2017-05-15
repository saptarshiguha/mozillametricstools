
"""
Utilities for interacting with Mozilla's data S3 buckets.
"""

import boto3

S3_PARQUET_BUCKET = "telemetry-parquet"
S3_METRICS_BUCKET = "mozilla-metrics"

## Access to high-level interface.
s3_resource = boto3.resource("s3")
## Access to underlying lower-level client interface.
## Operations like listing buckets are easier using this.
s3_client = s3_resource.meta.client


def join_s3_path(*args, **kwargs):
    """ Join path components into a valid S3 URI/path.

        This essentially joins the components with the separator "/", and
        prepends the identifier "s3://" if necessary. This is most clearly used
        in the form `join_s3_path(bucket, key)`, but can be used to join
        arbitrary path components.

        Keyword args:
        trailing_slash: boolean - should a "/" be added at the end of the
                        path?  Default is False.
        protocol: boolean - should the protocol identifier "s3://" be
                  prepended?  Default is True.

        Returns a string containing a valid S3 URI/path.
    """
    if not all([isinstance(a, basestring) for a in args]):
        raise ValueError("All path components must be strings.")
    if len(args) > 1 and any([a.startswith("s3://") for a in args[1:]]):
        raise ValueError("Path components should not include the S3 " +
                         "protocol identifier ('s3://'), except for the first.")

    prepend_protocol = kwargs.get("protocol", True)
    trailing_slash = kwargs.get("trailing_slash", False)
    if not args:
        ## Trailing slash is meaningless here.
        trailing_slash = False
        s3_path = ""
    else:
        ## If the final path component ends in a slash,
        ## it should be retained (will be stripped and added back later).
        if args[-1].endswith("/"):
            trailing_slash = True
        ## If the first component starts with the protocol identifier,
        ## it should be retained, but does not need to be added again.
        if args[0].startswith("s3://"):
            prepend_protocol = False
        path_components_stripped = [path_comp.strip("/") for path_comp in args]
        s3_path = "/".join(path_components_stripped)
    if prepend_protocol:
        s3_path = "s3://{}".format(s3_path)
    if trailing_slash:
        s3_path = "{}/".format(s3_path)
    return s3_path


S3_METRICS_HOME_PATH = join_s3_path(S3_METRICS_BUCKET, "user/")
S3_PARQUET_PATH = join_s3_path(S3_PARQUET_BUCKET, trailing_slash=True)
S3_MAIN_SUMMARY_BASE_PATH = join_s3_path(S3_PARQUET_PATH, "main_summary/")
S3_LONGITUDINAL_BASE_PATH = join_s3_path(S3_PARQUET_PATH, "longitudinal/")


def uri_to_bucket_key_pair(s3_uri):
    """ Extracts the bucket and key from a S3 URI.

        Returns a tuple of (<bucket>, <key>).
        Both will be empty strings if there is no path portion after the
        protocol identifier. The <key> entry will be the empty string if the
        path is of the form "s3://<bucket>/". Also, the <key> entry will retain
        any trailing "/".
    """
    if not isinstance(s3_uri, basestring):
        raise ValueError("S3 URI must be a single string.")
    s3_uri = s3_uri.strip()
    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 URI must start with 's3://'.")

    stripped_path = s3_uri[5:]
    path_components = stripped_path.split("/", 1)
    if len(path_components) == 1:
        ## No key.
        path_components.append("")
    return tuple(path_components)


def bucket_from_uri(s3_uri):
    """ Extracts the bucket from a S3 URI. """
    return uri_to_bucket_key_pair(s3_uri)[0]


def key_from_uri(s3_uri):
    """ Extracts the key portion (excluding bucket) from a S3 URI. """
    return uri_to_bucket_key_pair(s3_uri)[1]


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
    return join_s3_path(S3_MAIN_SUMMARY_BASE_PATH,
                        latest_ms_version,
                        trailing_slash=True)


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
    return join_s3_path(S3_LONGITUDINAL_BASE_PATH,
                        latest_longit_version,
                        trailing_slash=True)


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

