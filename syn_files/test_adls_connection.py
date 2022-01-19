import time
from datetime import datetime
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
adls_auth_method = 'serviceprincipal'
adls_account_name = 'ochdasynapseadls'
aad_tenant_id = '0ee60ac4-5059-4f1c-857c-a9c4d2daab23'
aad_client_id = 'e7807070-4a2d-4791-8a88-bd7b4b693d3a'
aad_client_secret = 'lOF7Q~rJl4lh3xCVNyh5koLXXzcM6xMgp9ApH'
adls_client_secret = ''
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
        logging.critical('Exception while establishing connection to ADLS - ' + str(error))
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