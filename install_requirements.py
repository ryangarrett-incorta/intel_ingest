#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import os
import sys
import platform

os_type = platform.dist()
os_dist = os_type[0]
os_version = os_type[1] 
os_number = os_version[0] 
repoPath = (os.path.join("/etc/yum.repos.d/mssql-release.repo"))

# fileYum = "yum_requirements.txt"
# fileObj = open(fileYum)
# yum_packages = fileObj.read().splitlines()
# fileObj.close()

# filePip = "pip_requirements.txt"
# fileObj = open(filePip)
# pip_packages = fileObj.read().splitlines()
# fileObj.close()

# subprocess.call(["pip", "install", "--upgrade", "pip"])
# subprocess.call(["pip", "install", "pygrok"])
# subprocess.call(["sudo", "yum", "remove", "unixODBC-utf16-devel"])

# for y in yum_packages:
#     subprocess.call(["sudo", "yum", "install", "-y", y])

# for p in pip_packages:
#     subprocess.call(["python3", "-m", "pip", "install", p])

if os_dist == "centos":
   curl_url = 'https://packages.microsoft.com/config/centos/' + os_number + '/prod.repo'
   curl_out = subprocess.getstatusoutput(curl_url)
    #subprocess.call(["sudo", curl_url, ">", repoPath])
   print(curl_url)
   print(curl_out)
   print('true')
elif os_dist == "red hat":
    curl_url = 'https://packages.microsoft.com/config/rhel/' + os_number + '/prod.repo'
    #subprocess.call(["sudo",curl_url, ">", "/etc/yum.repos.d/mssql-release.repo"])
    print(os_dist)
else:
    print('false')
    print(os_dist)
    print(type(os_dist))
    