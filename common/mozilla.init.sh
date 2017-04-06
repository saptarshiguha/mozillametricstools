
## This uses mozillas new emr-dfs persistant home directories
## so presumably you have installed the stuff in setupRhipe.sh
## already! 

## If for example you start the mozilla cluster without --efs-dns option
## you will have a blank home folder. You would then need to run

##
#                 cl  <- aws.step.run(cl, script=sprintf('s3://%s/run.user.script.sh',aws.options()$s3bucket)
#                           , args="https://raw.githubusercontent.com/saptarshiguha/mozaws/master/bootscriptsAndR/setupRhipe.sh"
#                           , name="Install RHIPE"
#                           , wait=TRUE)

# and

#         cl  <- aws.step.run(cl, script=sprintf('s3://%s/run.user.script.sh',aws.options()$s3bucket)
#                           , args="https://raw.githubusercontent.com/saptarshiguha/mozillametricstools/master/common/spark.init.step.sh"
#                           , name="Clone Our Repo"
#                           , wait=TRUE)
## these files dump stuff in the home folder which will persist and install some packages (which will always need to be reinstalled, as they wont persist)

## So what you need to do is run the cluster with efs-dns option set to true
## run the above shell files
## add this to bashrc

# export AWS_DEFAULT_REGION=$(curl --retry 5 --silent --connect-timeout 2 http://169.254.169.254/latest/dynamic/instance-identity/document | grep region | awk -F\" '{print $4}')
# export JAVA_HOME=/etc/alternatives/jre


cd ~/
sudo yum -y install protobuf-2.5.0-10.el7.centos.x86_64.rpm protobuf-compiler-2.5.0-10.el7.centos.x86_64.rpm protobuf-devel-2.5.0-10.el7.centos.x86_64.rpm


aws s3 cp s3://mozilla-metrics/share/R.tar.gz /tmp/
hadoop dfs -put  /tmp/R.tar.gz /

yum -y install inotify-tools

sudo yum -y install curl-devel
