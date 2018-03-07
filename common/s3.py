"""
Utilities for interacting with Mozilla's data S3 buckets.
"""

import boto3
import os.path

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


def dataset_latest_version_path(dataset):
    """ Returns the S3 path to the latest version of the given
        derived dataset. """
    dataset_versions = list_subkeys(S3_PARQUET_BUCKET,
                               prefix=dataset + "/",
                               last_component_only=True,
                               include_contents=False)
    if not dataset_versions:
        raise ValueError("No S3 paths found for the given dataset name.")

    ## version path components are generally of the form "v1", "v2", "v3",...
    ## We expect them to order lexicographically from oldest to newest.
    dataset_versions.sort(reverse=True)
    latest_version = dataset_versions[0]
    return join_s3_path(S3_PARQUET_PATH,
                        dataset,
                        latest_version,
                        trailing_slash=True)


def main_summary_path():
    """ Returns the S3 path to the latest version `main_summary` dataset. """
    return dataset_latest_version_path("main_summary")


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


def partition_key_paths(base_path, partition_cols, partition_values,
                                                            check_valid=True):
    """ Expand all paths corresponding to Parquet partition column values.

        Parquet partitioning writes partition column values as separate S3 keys.
        To filter on partition column values, it is efficient to read the
        DataFrame restricting to the corresponding S3 keys.

        Given lists of values for specified partition columns, returns a list
        of S3 keys corresponding to the Cartesian product of all partition
        values, with the base DataFrame path prepended. Optionally prunes out
        any nonexistent keys.

        base_path: the base S3 path for the DataFrame, up to the partition
                   column portions
        partition_cols: a list of partition column names (or single string) in
                        the order they appear in the S3 path
        partition_values: a list containing, for each partition column name in
                          partition_cols (in the same order), a list (or single
                          string) of column values
        check_valid: check whether each expanded path exists?
    """
    if not partition_cols:
        raise ValueError("No partition columns were specified.")
    if isinstance(partition_cols, basestring):
        partition_cols = [partition_cols]

    if not partition_values:
        raise ValueError("No partition values were specified.")
    if isinstance(partition_values, basestring):
        ## A single partition value for a single column.
        partition_values = [partition_values]
    if len(partition_cols) != len(partition_values):
        raise ValueError("Partition values list length must be the same as " +
                         "the number of partition columns.")
    for v in partition_values:
        if not v:
            raise ValueError("Partition values must be specified for each " +
                             "column.")
    partition_values = [[v] if isinstance(v, basestring) else v
                           for v in partition_values]

    def listprod(x, y):
        """ Compute the cartesian product of paths by combining segments from
            two lists.
        """
        joined_segments = []
        for xv in x:
            combined = [join_s3_path(xv, yv, protocol=False) for yv in y]
            joined_segments.extend(combined)
        return joined_segments

    partition_keys = []
    ## For each partition column and values, generate the corresponding path
    ## segment strings of the form "<col>=<value>".
    for i in range(len(partition_cols)):
        val_strings = ["{col}={val}".format(col=partition_cols[i], val=v)
                          for v in partition_values[i]]
        partition_keys.append(val_strings)
    ## Generate the cartesian product of path segments.
    partition_paths = reduce(listprod, partition_keys)

    if check_valid:
        ## Verify that each of these generated paths exists.
        base_bucket, base_prefix = uri_to_bucket_key_pair(base_path)
        valid_paths = []
        for p in partition_paths:
            full_prefix = join_s3_path(base_prefix, p, protocol=False)
            try:
                if list_subkeys(base_bucket, full_prefix):
                    valid_paths.append(p)
            except:
                ## If there is a problem accessing the path, consider it
                ## invalid.
                pass
        if not valid_paths:
            ## None of the paths were valid.
            return []
        partition_paths = valid_paths

    ## Prepend the base path before returning.
    return [join_s3_path(base_path, p, trailing_slash=True)
                for p in partition_paths]


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


def _get_bucket_key_from_args(uri_or_bucket, key=None):
    """ Extract the (bucket, key) pair represented by the given args and check
        for validity.

        The (bucket, key) pair can be identified either by a single URI or by
        separate strings for bucket and key. In the first case, the first arg
        should be a valid S3 URI, and the second arg should be omitted. In the
        second case, both args must be used, and the first arg must be a valid
        bucket name.

        Returns a tuple of (<bucket>, <key>).

        Throws an exception if the given information did not represent valid S3
        identifiers.
    """
    if not isinstance(uri_or_bucket, basestring):
        raise ValueError("First arg must be a string.")
    if key and not isinstance(key, basestring):
        raise ValueError("Key must be a string.")

    if not key:
        key_given = False
        ## The first arg should be a proper URI.
        bucket, key = uri_to_bucket_key_pair(uri_or_bucket)
    else:
        key_given = True
        bucket = uri_or_bucket

    bucket = bucket.strip()
    key = key.strip()
    key = key.lstrip("/")
    if not bucket:
        err_val = "First arg (bucket name)" if key_given \
            else "Bucket portion of the URI"
        raise ValueError("{} cannot be empty.".format(err_val))
    if not key:
        err_val = "Key" if key_given else "Key portion of the URI"
        raise ValueError("{} cannot be empty.".format(err_val))

    ## The bucket should not have the 's3://' prefix and should not contain
    ## any slashes.
    if bucket.startswith("s3://"):
        raise ValueError("The first arg should be the bucket name and " +
                         "should not start with the 's3://' prefix.")
    if "/" in bucket:
        raise ValueError("The bucket does not have a valid name.")

    return bucket, key


def s3_download(uri_or_bucket, key=None, local_path=None):
    """ Download a file from S3.

        The S3 object to download can be identified either by a single URI or
        by a (bucket, key) pair. In the second case, the first two args must
        both be used, and the first arg must be a valid bucket name (not a
        path).

        If `local_path` is specified, the file will be downloaded to the given
        path (defaulting to the same filename in the current dir).
    """
    if local_path and not isinstance(local_path, basestring):
        raise ValueError("Local path must be a string.")
    ## Check S3 identifiers for validity, and let any exceptions
    ## be raised.
    bucket, key = _get_bucket_key_from_args(uri_or_bucket, key)
    ## The key should not end with a slash.
    if key.endswith("/"):
        raise ValueError("The key should not end in a slash ('/') " +
                         "-- this usually indicates a S3 key prefix.")
    if not local_path:
        ## Default to the filename at the end of the S3 key.
        key_filename = key.rsplit("/", 1)[-1]
        local_path = "./{}".format(key_filename)

    print("Downloading {} to {}".format(join_s3_path(bucket, key), local_path))
    ## Try the download, and let any exceptions be raised.
    s3_client.download_file(Bucket=bucket,
                            Key=key,
                            Filename=local_path)


def s3_upload(local_path, uri_or_bucket, key=None):
    """ Upload a file to S3.

        The S3 object to upload to can be identified either by a single URI or
        by a (bucket, key) pair. In the second case, the last two args must
        both be used, and the first of those must be a valid bucket name (not a
        path).

        If the key ends in a slash ("/"), this will be interpreted as a prefix,
        and the local filename will be appended.
    """
    if not isinstance(local_path, basestring):
        raise ValueError("Local path must be a string.")
    local_filename = os.path.basename(local_path)
    ## Do a cursory check that the local path points to a file.
    ## This is convenient because the filename is used later. Any other
    ## IO errors will be thrown by the S3 Client method.
    if not local_filename:
        raise ValueError("Local path must be a file, not a dir.")
    ## Check S3 identifiers for validity, and let any exceptions
    ## be raised.
    bucket, key = _get_bucket_key_from_args(uri_or_bucket, key)
    ## If the key ends with a slash, interpret it as a prefix.
    if key.endswith("/"):
        key = key + local_filename

    print("Uploading {} to {}".format(local_path, join_s3_path(bucket, key)))
    ## Try the download, and let any exceptions be raised.
    s3_client.upload_file(Filename=local_path,
                          Bucket=bucket,
                          Key=key)

