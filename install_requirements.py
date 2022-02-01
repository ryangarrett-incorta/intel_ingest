#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import os
import sys
import platform
import shutil

incorta_home = os.getenv('INCORTA_HOME')

osType = platform.dist()
osDist = osType[0]
osVersion = osType[1] 
os_number = osVersion[0] 
repoPath = (os.path.join("/etc/yum.repos.d/mssql-release.repo"))

fileYum = "yum_requirements.txt"
fileObj = open(fileYum)
yumPackages = fileObj.read().splitlines()
fileObj.close()

filePip = "pip_requirements.txt"
fileObj = open(filePip)
pipPackages = fileObj.read().splitlines()
fileObj.close()

subprocess.call(["pip", "install", "--upgrade", "pip"])
subprocess.call(["sudo", "yum", "remove", "unixODBC-utf16-devel"])
subprocess.call(["sudo", "ACCEPT_EULA=Y", "yum", "install", "-y", "msodbcsql17"])

for y in yumPackages:
    subprocess.call(["sudo", "yum", "install", "-y", y])

for p in pipPackages:
    subprocess.call(["python3", "-m", "pip", "install", p])

if osDist == "centos":
    repo_url = 'https://packages.microsoft.com/config/centos/' + os_number + '/prod.repo'
    subprocess.call(["curl",repo_url])
    print('true')
   
elif osDist == "red hat":
    curl_url = 'https://packages.microsoft.com/config/rhel/' + os_number + '/prod.repo'
    print(osDist)
else:
    print('false')
    print(osDist)
    print(type(osDist))
    