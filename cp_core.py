#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import argparse

incorta_home = os.getenv('INCORTA_HOME')

parser = argparse.ArgumentParser(description='Incorta copy core-site.xml.  Usage: cp_core.py source_xml')
parser.add_argument('source_file', type=str, help='Path to Input File example: /home/core-site.xml')
args = parser.parse_args()
source_file_arg = args.source_file.split(",")
source_file = os.path.join(*source_file_arg)

# copy locations
loc1 = (os.path.join(incorta_home, "cmc/lib/"))
loc2 = (os.path.join(incorta_home, "cmc/tmt/"))
loc3 = (os.path.join(incorta_home, "IncortaNode/hadoop/etc/hadoop/"))
loc4 = (os.path.join(incorta_home, "IncortaNode/runtime/lib/"))
loc5 = (os.path.join(incorta_home, "IncortaNode/runtime/webapps/incorta/WEB-INF/lib/"))

#locations array
locations = [loc1,loc2,loc3,loc4,loc5]

#copy files
def cp_Core():
    for l in locations:
        try:
            shutil.copy(source_file, l)
            print("Copied " + source_file + " to " + l)
        except:
            print("Error occurred while copying file: " + l)

cp_Core()