# https://cloud.google.com/iot/docs/samples/device-manager-samples

import datetime as dt
from google.oauth2 import service_account
from googleapiclient import discovery, errors

from .env_vars import *


# ------------------------------------------------------------------------------
# Returns an authorized API client by discovering the IoT API
# using the service account credentials JSON file.
def get_IoT_client(path_to_service_account_json):
    api_scopes = ['https://www.googleapis.com/auth/cloud-platform']
    api_version = 'v1'
    discovery_api = 'https://cloudiot.googleapis.com/$discovery/rest'
    service_name = 'cloudiotcore'

    creds = service_account.Credentials.from_service_account_file(
        path_to_service_account_json)
    scoped_credentials = creds.with_scopes(api_scopes)

    discovery_url = '{}?version={}'.format(
        discovery_api, api_version)

    return discovery.build(
        service_name,
        api_version,
        discoveryServiceUrl=discovery_url,
        credentials=scoped_credentials)


# Get an IoT client using the GCP project (NOT firebase proj!)
iot_client = get_IoT_client(path_to_google_service_account)


#------------------------------------------------------------------------------
# Get the count of IoT registrations.
def get_iot_registrations():

    # path to the device registry
    registry_name = 'projects/{}/locations/{}/registries/{}'.format(
        cloud_project_id, cloud_region, device_registry)

    try:
        # get devices registry and list
        devices = iot_client.projects().locations().registries().devices()
        list_of_devices = devices.list(parent=registry_name).execute(
                ).get('devices', [])
    except errors.HttpError as e:
        print('get_iot_registrations: ERROR: '
              'HttpError: {}'.format(e._get_reason()))
        return False

    res = {}
    res['registered'] = "{:,}".format(len(list_of_devices))
    res['timestamp'] = dt.datetime.utcnow().strftime('%FT%XZ')
    return res


#------------------------------------------------------------------------------
# Return a dict with a list of IoT devices with heartbeat and metadata.
def get_iot_device_list():

    # path to the device registry
    registry_name = 'projects/{}/locations/{}/registries/{}'.format(
        cloud_project_id, cloud_region, device_registry)

    try:
        # get devices registry and list
        devices = iot_client.projects().locations().registries().devices()
        list_of_devices = devices.list(parent=registry_name).execute(
                ).get('devices', [])
    except errors.HttpError as e:
        print('get_iot_device_list: ERROR: '
              'HttpError: {}'.format(e._get_reason()))
        return False

    res = {}
    res['devices'] = [] # list of devices
    for d in list_of_devices:
        device_id = d.get('id')
        device_name = '{}/devices/{}'.format(registry_name, device_id)
        device = devices.get(name=device_name).execute()
        last_heartbeat_time = device.get('lastHeartbeatTime', '')
        last_config_send_time = device.get('lastConfigSendTime', 'Never')
        last_error_time = device.get('lastErrorTime', '')
        last_error_message = device.get('lastErrorStatus', {})
        last_error_message = last_error_message.get('message', '')
        metadata = device.get('metadata', {})
        user_uuid = metadata.get('user_uuid', None)
        device_notes = metadata.get('device_notes', '')
        device_name = metadata.get('device_name', '')

        dev = {}
        dev['device_uuid'] = device_id # MUST use key 'device_uuid' to match DS
        dev['last_heartbeat_time'] = last_heartbeat_time
        dev['last_error_time'] = last_error_time
        dev['last_error_message'] = last_error_message
        dev['last_config_send_time'] = last_config_send_time # last recipe sent
        dev['user_uuid'] = user_uuid
        dev['device_notes'] = device_notes
        dev['device_name'] = device_name

        res['devices'].append(dev)

    res['timestamp'] = dt.datetime.utcnow().strftime('%FT%XZ')
    return res


#------------------------------------------------------------------------------
# Delete a device, returns result from google API.
def delete_iot_device(device_id):

    # path to the device registry & device
    registry_name = 'projects/{}/locations/{}/registries/{}'.format(
        cloud_project_id, cloud_region, device_registry)
    device_name = '{}/devices/{}'.format(registry_name, device_id)

    try:
        # get devices registry 
        devices = iot_client.projects().locations().registries().devices()
        devices.delete(name=device_name).execute()
        return True
    except errors.HttpError as e:
        print('delete_iot_device: ERROR: '
              'HttpError: {}'.format(e._get_reason()))
    return False





