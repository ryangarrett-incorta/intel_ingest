#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import argparse
from datetime import datetime
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential

adls_auth_method = 'serviceprincipal'

adls_account_name = ''
aad_tenant_id = ''
aad_client_id = ''
aad_client_secret = ''
adls_client_secret = ''

if __name__ == "__main__":
   
    parser = argparse.ArgumentParser(description='Azure App Details')
    parser.add_argument('-a', '--a', type=str, help='Azure App Account Name')
    parser.add_argument('-t','--t', type=str, help='Azure App Tenant ID')
    parser.add_argument('-c','--c', type=str, help='Azure App Client ID')
    parser.add_argument('-s','--s', type=str, help='Azure App Client Secret')
    
    args = parser.parse_args()
    adls_account_name = args.a
    aad_tenant_id = args.t
    aad_client_id = args.c
    aad_client_secret = args.s
    print(adls_account_name,aad_tenant_id,aad_client_id,aad_client_secret)
    
def create_adls_client():
    if adls_auth_method == 'serviceprincipal':
        credential = ClientSecretCredential(aad_tenant_id, aad_client_id, aad_client_secret)
    else:
        credential = "{}".format(adls_client_secret)
    try:
        adls_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", adls_account_name),
            credential=credential
        )
        return adls_client
    except Exception as error:
        #logging.critical('Exception while establishing connection to ADLS - ' + str(error))
        print("Exception while establishing connection to ADLS" + str(error))
def list_directory_contents(service_client):
    try:
        
        file_system_client = service_client.get_file_system_client(file_system="tenants")
        paths = file_system_client.get_paths(path="incorta")
        for path in paths:
            print(path.name + '\n')
    except Exception as e:
      print(e)

service_client = create_adls_client()
list_directory_contents(service_client)