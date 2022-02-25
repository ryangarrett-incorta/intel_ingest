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

# analyticsServices = []
# loaderServices = []
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

subprocess.call(["pip3", "install", "--upgrade", "pip"])
subprocess.call(["pip3", "install", "--user", "pyodbc"])
subprocess.call(["sudo", "yum", "remove", "unixODBC-utf16-devel"])
subprocess.call(["sudo", "ACCEPT_EULA=Y", "yum", "install", "-y", "msodbcsql17"])

for p in pipPackages:
    subprocess.call(["python3", "-m", "pip", "install", p])

# create syn folders
syn_path = (os.path.join(incorta_home, "IncortaNode/syn/"))
syn_logpath = (os.path.join(incorta_home, "IncortaNode/syn/logs"))
synapse_mappings = "./synapse_mappings.csv"

try:
    os.makedirs(syn_logpath)
    print ("syn path created: ", syn_logpath)
except FileExistsError:
    print ("Synpath previosly created, skipping")
    pass

# copy synapse mapping
shutil.copy(synapse_mappings,syn_path)
print("Copied " + synapse_mappings + " to " + syn_path)

# copy egg file
egg_file = glob.glob('./*.egg')
egg_file_str = egg_file[0]
egg_file_path = (os.path.join(incorta_home, "IncortaNode/incorta.ml/lib"))

shutil.copy(egg_file_str,egg_file_path)
print("Copied " + egg_file_str + " to " + egg_file_path)

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

stop_all_incorta_services.restart_All()
    