
## the following lines have been commented because i ran them once and because of the 
## new persistant folders i dont need to run them again.
## in an ideal world, this could would check for a flag: do i need to run or not and then bail ...
## but who cares?

#cd ~/
#IS_MASTER=false
#if [ -f /mnt/var/lib/info/instance.json ]
#then
#	IS_MASTER=$(jq .isMaster /mnt/var/lib/info/instance.json)
#fi
#if $IS_MASTER; then
#    ## only runs on master
#  rm -rf /home/hadoop/mozillametricstools   
#  git clone https://github.com/saptarshiguha/mozillametricstools   
#fi 

#echo `ps -o user= -p $$ | awk '{print $1}'`
#ipython profile create
## Copy the ipython startup files
#mkdir -p ~/.ipython/profile_default/startup/ && cp ~/mozillametricstools/01-mozmetrics-setup.py ~/.ipython/profile_default/startup/

## anaconda is now set from this code
## https://github.com/mozilla/emr-bootstrap-spark/blob/master/ansible/envs/dev.yml#L6
## these i always run since they do not install into a home folder
/mnt/anaconda2/bin/pip install py4j --upgrade
/mnt/anaconda2/bin/pip install feather-format


