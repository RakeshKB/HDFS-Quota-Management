#!/bin/bash

#Copy source code for BootstrapHDFS project present in the root
mkdir -p /opt/inmobi/BootstrapHDFS
cp -r api_server index.html operations poller www /opt/inmobi/BootstrapHDFS/.

#Install dependencies
#pip install -r /opt/inmobi/BootstrapHDFS/api_server/requirements.txt 

#Copy upstart conf
cp operations/BootstrapHDFS.conf /etc/init/BootstrapHDFS.conf

#Restart app
stop BootstrapHDFS
start BootstrapHDFS

#Copy CRON jobs
cp operations/BootstrapHDFS /etc/cron.d

