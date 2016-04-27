def latest_longitudinal_path():
    ## use boto,
    ## conn = boto.connect_s3(host="s3-us-west-2.amazonaws.com")
    import subprocess,re,operator
    longitudinal_basepath = "s3://telemetry-parquet/longitudinal/"
    p = subprocess.Popen("aws s3 ls "+longitudinal_basepath,shell=True, stdout=subprocess.PIPE).stdout.readlines()
    def g(x):
        if x:
            return x.groups()[0]
        else:
            return '00000000'
    re1 = re.compile(r""" +PRE (v\d{8})""",0)
    b = [g(re1.match(s)) for s  in p]
    value = __builtin__.max(b)
    return longitudinal_basepath+value
    

