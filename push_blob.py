  
import argparse
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
from os import listdir
from os.path import isfile, join
import re
from tqdm import tqdm


def push_blob_f(video_id = None, container = None, basepath = None):
    upload_path = f'{video_id}'
    content_type = 'video/mp4'
    container = container
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=videobank;AccountKey=+7+BZaxs5zBHwyDAMJHnMEJS1mhzIN4AC6PS7wIbVgE1hd35eHEB9IAbc+E2PfV4GNP7dkFrWiLAVMZ8HgnFEw==;EndpointSuffix=core.windows.net'
    # files = [f for f in listdir(args.input_path) if isfile(join(args.input_path, f))]

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)
    contentType = ContentSettings(content_type=content_type)
    basepath = '.'

    dest = f"{upload_path}/{video_id}.mp4"
    blob_client = container_client.get_blob_client(dest)
    try:
        with open(f"{basepath}/{video_id}.mp4", "rb") as data:
            print(f"{basepath}/{video_id}.mp4 ->{upload_path}/{video_id}.mp4")
            blob_client.upload_blob(data, overwrite=True, content_settings=contentType)
            print(f'{"<"*10} upload completed {">"*10}')

    except Exception as e:
        print(e)
        print("Upload failed")


#push_blob_f(video_id='paswan', basepath='.', container='var')