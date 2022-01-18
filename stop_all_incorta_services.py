#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import argparse

parser = argparse.ArgumentParser(description='Stop.Start.Restart Incorta Services  Assumes $INCORTA_HOME is set Usage: stop_all_incorta_services.py --stop')
parser.add_argument('-s','--stop', action="store_true", help='Stop All Services')
parser.add_argument('-t','--start', action="store_true", help='Start All Services')
parser.add_argument('-r','--restart', action="store_true", help='Restart All Services')

args = vars(parser.parse_args())

incorta_home = os.getenv('INCORTA_HOME')
file_args = ""

def stop_Spark():
    stopSpark = (os.path.join(incorta_home, "IncortaNode/stopSpark.sh"))
    print("Stopping Spark Services ..." )
    subprocess.call([stopSpark])    

def start_Spark():
    startSpark = (os.path.join(incorta_home, "IncortaNode/startSpark.sh"))
    print("Starting Spark Services ..." )
    subprocess.call([startSpark])    

def stop_Loader(file_args):
    stopLoader = (os.path.join(incorta_home, "IncortaNode/stopService.sh"))
    file_args = "loaderService"
    print("Stopping Loader Services ..." )
    subprocess.call([stopLoader,file_args])
 
def start_Loader(file_args):
    startLoader = (os.path.join(incorta_home, "IncortaNode/startService.sh"))
    file_args = "loaderService"
    print("Starting Loader Services ..." )
    subprocess.call([startLoader,file_args])

def stop_Analytics(file_args):
    stopAnalytics = (os.path.join(incorta_home, "IncortaNode/stopService.sh"))
    file_args = "analyticsService"
    print("Stopping Analytics Services ..." )
    subprocess.call([stopAnalytics,file_args])

def start_Analytics(file_args):
    startAnalytics = (os.path.join(incorta_home, "IncortaNode/startService.sh"))
    file_args = "analyticsService"
    print("Starting Analytics Services ..." )
    subprocess.call([startAnalytics,file_args])
    
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
    stop_Analytics(file_args)
    stop_Loader(file_args)
    stop_Spark()
    
def start_All():
    start_CMC()
    start_Node()
    start_Analytics(file_args)    
    start_Loader(file_args)   
    start_Spark()
    
def restart_All():
    stop_All()
    start_All()

if (args['stop']):
    stop_All()

elif (args['start']):
    start_All()

elif (args['restart']):
    restart_All()

else:
    print ("Please check your flags & Try Again.  Example Usage /home/install_files/stop_all_incorta_services.py -s")