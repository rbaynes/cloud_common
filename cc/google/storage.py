# https://google-cloud-python.readthedocs.io/en/stable/storage/client.html

from google.cloud import storage

from .env_vars import *

# Storage client for Google Cloud
storage_client = storage.Client(cloud_project_id)

DEBIAN_PACKAGE_BUCKET = 'openag-v1-debian-packages'
IMAGE_BUCKET = 'openag-v1-images'

URL_TEMPLATE = 'https://console.cloud.google.com/storage/browser/{}?project=openag-v1'

#------------------------------------------------------------------------------
def get_latest_debian_package_from_storage():
    try:
        bucket = storage_client.get_bucket(DEBIAN_PACKAGE_BUCKET)
        blobs = list(bucket.list_blobs())
        # Blobs seem to come back in the order you'd expect. 
        # So take the last one that ends in '.deb'
        latest = ''
        for blob in blobs:
            if blob.name.endswith('.deb'):
                latest = blob.name 
        # pool/main/o/openagbrain/openagbrain_1.0-4_armhf.deb
        # trim up to the last '/'
        return latest[latest.rfind('/')+1:]
    except google.cloud.exceptions.NotFound:
        print('ERROR: bucket does not exist: ',DEBIAN_PACKAGE_BUCKET)
    return None # no data


#------------------------------------------------------------------------------
def get_latest_backup_from_storage():
    try:
        buckets = list(storage_client.list_buckets(prefix='openag-v1-backup-'))
        return buckets[-1].name
    except:
        print('ERROR: no backup buckets.')
    return None # no data


#------------------------------------------------------------------------------
def get_images_URL_from_storage():
    return URL_TEMPLATE.format(IMAGE_BUCKET)

