cd ~/
IS_MASTER=false
if [ -f /mnt/var/lib/info/instance.json ]
then
	IS_MASTER=$(jq .isMaster /mnt/var/lib/info/instance.json)
fi
if $IS_MASTER; then
    ## only runs on master
  git clone https://github.com/saptarshiguha/mozillametricstools   
fi 

sudo pip install --upgrade pip
sudo pip install py4j --upgrade
sudo pip install feather-format
## Copy the ipython startup files

mkdir -p ~/.ipython/profile_default/startup/ && cp ~/mozillametricstools/01-mozmetrics-setup.py ~/.ipython/profile_default/startup/
