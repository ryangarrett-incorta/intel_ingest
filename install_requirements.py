#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import os
import sys
import platform
import shutil
import time
import glob
import re
import argparse
import stop_all_incorta_services

incorta_home = os.getenv('INCORTA_HOME')

os_distribution = platform.linux_distribution()
os_type = os_distribution[0]
os_version = os_distribution[1]
os_build = os_version[0]
os_system = platform.system()

if 'Linux' in os_system:
    pass
else:
    print("Not supported OS.... Exiting script")
    exit(1)

if incorta_home is None:
    print("Please set INCORTA_HOME in bashrc.... Exiting script")
    exit(1)
else:
    print("INCORTA_HOME = " + incorta_home)

parser = argparse.ArgumentParser(description='Incorta copy core-site.xml.  Usage: cp_core.py source_xml')
parser.add_argument('source_file', type=str, help='Path to Input File example: /home/core-site.xml')

args = parser.parse_args()
source_file_arg = args.source_file.split(",")
source_file = os.path.join(*source_file_arg)

# set intelligent.ingest.enabled
services_index = (os.path.join(incorta_home, "IncortaNode/services/services.index"))

with open(services_index) as f:
    lines = f.readlines() 

if len(lines) == 0:
     print("No Analytics or Loader Services found, please create loader and analytcis services.... Exiting")
     exit(1)
else:
    pass
    
servicesID = []

for l in lines:
    if '=' in str(l):
        services = re.sub('.*=', '', l).strip() 
        servicesFile = (os.path.join(incorta_home, "IncortaNode/services/", services, "incorta/service.properties"))
        servicesID.append(servicesFile)
    else:
        pass
        
for s in servicesID:
    with open(s, "r") as file_object:
        if 'intelligent.ingest.enabled = true' in file_object.read():
            print ("intel ingest already enabled on ", s)
        elif 'intelligent.ingest.enabled = false' in file_object.read():
            print ("intel ingest set to false on, please set this manually ", s)
            lines = file_object.readlines()
            print("false", s)
            # with open(s, "w") as fo:
            #     for line in lines:
            #         if line.strip("\n") != "intelligent.ingest.enabled = false":
            #             fo.write(line)
            # file_object.write("intelligent.ingest.enabled = true")
            # print ("intel ingest set to false on ", s)
        else:
            with open(s, "a") as file_object:
                file_object.write("intelligent.ingest.enabled = true")
                print("intelligent ingest enabled on ", s)

# cp ms repo path
ms_repo_file = glob.glob('./*.repo')
ms_repo_file_str = ms_repo_file[0]
ms_repo_path = "/etc/yum.repos.d/"

try:
    shutil.copy(ms_repo_file_str,ms_repo_path)
    print("Copied " + ms_repo_file_str + " to " + ms_repo_path)
except PermissionError:
    subprocess.call(['sudo', 'cp', '{0}'.format(ms_repo_file_str), '{0}'.format(ms_repo_path)])
    print("Sudo Copied " + ms_repo_file_str + " to " + ms_repo_path)

# set epel repo
epel_link = "https://dl.fedoraproject.org/pub/epel/epel-release-latest-" + os_build + ".noarch.rpm"
subprocess.call(["sudo", "dnf", "install", epel_link, "-y"])
print("Setting epel to " +  epel_link)

subprocess.call(["pip3", "install", "--upgrade", "pip"])
subprocess.call(["pip3", "install", "--user", "pyodbc"])
subprocess.call(["sudo", "yum", "remove", "unixODBC-utf16-devel"])
subprocess.call(["sudo", "ACCEPT_EULA=Y", "yum", "install", "-y", "msodbcsql17"])

# install yum and pip requirements
fileYum = "yum_requirements.txt"
fileObj = open(fileYum)
yumPackages = fileObj.read().splitlines()
fileObj.close()

filePip = "pip_requirements.txt"
fileObj = open(filePip)
pipPackages = fileObj.read().splitlines()
fileObj.close()

for y in yumPackages:
    subprocess.call(["sudo", "yum", "install", "-y", y])

for p in pipPackages:
    subprocess.call(["python3", "-m", "pip", "install", p])

print("Installing Incorta Ingest Wheel")
incorta_wheel_path = (os.path.join(incorta_home, "IncortaNode/Synapse/pkg/*.whl"))
incorta_wheel = glob.glob(incorta_wheel_path)
subprocess.call(["python3", "-m", "pip", "install", incorta_wheel[0]])

# cp core_site.xml
loc1 = (os.path.join(incorta_home, "cmc/lib/"))
loc2 = (os.path.join(incorta_home, "cmc/tmt/"))
loc3 = (os.path.join(incorta_home, "IncortaNode/hadoop/etc/hadoop/"))
loc4 = (os.path.join(incorta_home, "IncortaNode/runtime/lib/"))
loc5 = (os.path.join(incorta_home, "IncortaNode/runtime/webapps/incorta/WEB-INF/lib/"))

#locations array
locations = [loc1,loc2,loc3,loc4,loc5]
for l in locations:
    try:
        shutil.copy(source_file, l)
        print("Copied " + source_file + " to " + l)
    except:
        print("Error occurred while copying file: " + l)

#remove wildfly 1.0.4
if '8' in os_build:
    wildfly107 = (os.path.join(incorta_home, "IncortaNode/runtime/webapps/incorta/WEB-INF/lib/wildfly-openssl-1.0.7.Final.jar"))
    wildfly104 = (os.path.join(incorta_home, "IncortaNode/hadoop/share/hadoop/tools/lib/wildfly-openssl-1.0.4.Final.jar"))
    wildfly_loc = (os.path.join(incorta_home, "IncortaNode/hadoop/share/hadoop/tools/lib"))
    try:
        shutil.copy(wildfly107, wildfly_loc)
        os.chmod((os.path.join(wildfly_loc, "wildfly-openssl-1.0.7.Final.jar")), 777)
        print("Copied " + wildfly107 + " to " + wildfly_loc)
    except:
         print("Error occurred while copying file: " + wildfly107)
    try:
        os.remove(wildfly104)
        print(wildfly104 + " Deleted")
    except:
        print ("wildfly 1.0.4 not found")

print("Setting INCORTA_USE_AZURE_APIS=true")
with open(os.path.expanduser("~/.bashrc"), "a") as outfile:  
    outfile.write("export INCORTA_USE_AZURE_APIS=true")
os.system("source ~/.bashrc")

print ("Restarting All Incorta Services")
stop_all_incorta_services.restart_All()
    