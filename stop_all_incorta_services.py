#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import argparse
import re

incorta_home = os.getenv('INCORTA_HOME')

#get service names
services_index = (os.path.join(incorta_home, "IncortaNode/services/services.index"))

with open(services_index) as f:
    lines = f.readlines() 
if len(lines) == 0:
     print("No Analytics or Loader Services found, please create loader and analytcis services")
else:
    pass

serviceNames = []
for l in lines:
    if '=' in str(l):
        services = re.sub('=.*', '', l).strip() 
        serviceNames.append(services)
    else:
        pass

def stop_Spark():
    stopSpark = (os.path.join(incorta_home, "IncortaNode/stopSpark.sh"))
    print("Stopping Spark Services ..." )
    subprocess.call([stopSpark])    

def start_Spark():
    startSpark = (os.path.join(incorta_home, "IncortaNode/startSpark.sh"))
    print("Starting Spark Services ..." )
    subprocess.call([startSpark]) 

def stop_Services():
    stopServices = (os.path.join(incorta_home, "IncortaNode/stopService.sh"))
    for s in serviceNames:
        print("Stopping ",s)
        subprocess.call([stopServices,s,"-force"])

def start_Services():
    startServices = (os.path.join(incorta_home, "IncortaNode/startService.sh"))
    for s in serviceNames:
        print("Starting ",s)
        subprocess.call([startServices,s])
    
def stop_Node():
    stopNode = (os.path.join(incorta_home, "IncortaNode/stopNode.sh"))
    print("Stopping Node Services ..." )
    subprocess.call([stopNode])
   
def start_Node():
    startNode = (os.path.join(incorta_home, "IncortaNode/startNode.sh"))
    print("Starting Node Services ..." )
    subprocess.call([startNode])
    
def stop_CMC():
    stopCMC = (os.path.join(incorta_home, "cmc/stop-cmc.sh"))
    print("Stopping CMC ..." )
    subprocess.call([stopCMC])

def start_CMC():
    startCMC = (os.path.join(incorta_home, "cmc/start-cmc.sh"))
    print("Starting CMC ..." )
    subprocess.call([startCMC])
    
def stop_All():
    stop_CMC()
    stop_Node()
    stop_Services()
    stop_Spark()
    
def start_All():
    start_CMC()
    start_Node()
    start_Services()   
    start_Spark()
    
def restart_All():
    stop_All()
    start_All()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stop.Start.Restart Incorta Services  Assumes $INCORTA_HOME is set Usage: stop_all_incorta_services.py --stop')
    parser.add_argument('-s','--stop', action="store_true", help='Stop All Services')
    parser.add_argument('-t','--start', action="store_true", help='Start All Services')
    parser.add_argument('-r','--restart', action="store_true", help='Restart All Services')

    args = vars(parser.parse_args())

    if (args['stop']):
        stop_All()

    elif (args['start']):
        start_All()

    elif (args['restart']):
        restart_All()

    else:
        print ("Please check your flags & Try Again.  Example Usage /home/install_files/stop_all_incorta_services.py -s")
