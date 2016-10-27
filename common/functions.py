import datetime, time

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
    value = max(b)
    return (longitudinal_basepath+value,b)

def latest_executive_summary():
    ## see https://mana.mozilla.org/wiki/display/CLOUDSERVICES/Executive+Summary+Schema
    import subprocess,re,operator
    exec_basepath = "s3://telemetry-parquet/executive_stream/v3/"
    p = subprocess.Popen("aws s3 ls "+exec_basepath,shell=True, stdout=subprocess.PIPE).stdout.readlines()
    def g(x):
        if x:
            return x.groups()[0]
        else:
            return '00000000'
    re1 = re.compile(r""" +PRE (submission_date_s3=\d{8})""",0)
    b = [g(re1.match(s)) for s  in p]
    value = max(b)
    return exec_basepath+value



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

