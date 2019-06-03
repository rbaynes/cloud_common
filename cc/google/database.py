# All common database code.  From BigQuery and Datastore.

from queries import queries

from .bigquery import *
from .datastore import *


# NOTE: The XX_from_BQ() methods are only used if there is no data found
# in the Datastore.   


# ------------------------------------------------------------------------------
#debugrob: this is in ../utils.py, import that and use it.
def _bytes_to_string(bs):
    if isinstance(bs, bytes):
        bs = bs.decode('utf-8')
    return bs


# ------------------------------------------------------------------------------
# Get the historical CO2 values for this device.  
# Returns a list.
def get_co2_history(device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return []

    # First, try to get the data from the datastore...
    device_data = get_by_key_from_DS(DS_device_data_KEY, device_uuid)
    if device_data is None or DS_co2_KEY not in device_data:
        # If we didn't find any data in the DS, look in BQ...
        return get_co2_history_from_BQ(device_uuid)

    # process the vars list from the DS into the same format as BQ
    results = []
    valuesList = device_data[DS_co2_KEY]
    for val in valuesList:
        ts = _bytes_to_string(val['timestamp'])
        value = _bytes_to_string(val['value'])
        results.append({'value': value, 'time': ts})
    return results



# ------------------------------------------------------------------------------
# Get a list of the led panel historical values.
# Returns a list.
def get_led_panel_history(device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return []

    # First, try to get the data from the datastore...
    device_data = get_by_key_from_DS(DS_device_data_KEY, device_uuid)
    if device_data is None or DS_led_KEY not in device_data:
        # If we didn't find any data in the DS, look in BQ...
        return get_led_panel_history_from_BQ(device_uuid)

    # process the vars list from the DS into the same format as BQ
    results = []
    valuesList = device_data[DS_led_KEY]
    for val in valuesList:
        led_json = _bytes_to_string(val['value'])
        results.append(led_json)
    return results


# ------------------------------------------------------------------------------
# Get a dict with two arrays of the temp and humidity historical values.
# Returns a dict.
def get_temp_and_humidity_history(device_uuid):
    humidity_array = []
    temp_array = []
    result_json = {
        'RH': humidity_array,
        'temp': temp_array
    }
    if device_uuid is None or device_uuid is 'None':
        return result_json

        # First, try to get the data from the datastore...
    device_data = get_by_key_from_DS(DS_device_data_KEY, device_uuid)
    if device_data is None or \
            (DS_temp_KEY not in device_data and \
                         DS_rh_KEY not in device_data):
        # If we didn't find any data in the DS, look in BQ...
        return get_temp_and_humidity_history_from_BQ(device_uuid)

    # process the vars list from the DS into the same format as BQ

    # Get temp values
    if DS_temp_KEY in device_data:
        valuesList = device_data[DS_temp_KEY]
        for val in valuesList:
            ts = _bytes_to_string(val['timestamp'])
            value = _bytes_to_string(val['value'])
            result_json["temp"].append({'value': value, 'time': ts})

    # Get RH values
    if DS_rh_KEY in device_data:
        valuesList = device_data[DS_rh_KEY]
        for val in valuesList:
            ts = _bytes_to_string(val['timestamp'])
            value = _bytes_to_string(val['value'])
            result_json["RH"].append({'value': value, 'time': ts})

    return result_json


# ------------------------------------------------------------------------------
# Generic function to return a float value from DeviceData[key]
def get_current_float_value_from_DS(key, device_uuid):
    if device_uuid is None or device_uuid is 'None':
        return None

    device_data = get_by_key_from_DS(DS_device_data_KEY, device_uuid)
    if device_data is None or key not in device_data:
        return None

    # process the vars list from the DS into the same format as BQ
    result = None
    valuesList = device_data[key]
    val = valuesList[0]  # the first item in the list is most recent
    result = "{0:.2f}".format(float(val['value']))
    return result


# ------------------------------------------------------------------------------
# Get the current CO2 value for this device.  
# Returns a float or None.
def get_current_CO2_value(device_uuid):
    # First: look in the Datastore Device data dict...
    result = get_current_float_value_from_DS(DS_co2_KEY, device_uuid)
    if result is not None:
        return result

    # Second: do a big (slow) query
    return get_current_float_value_from_BQ(
        queries.fetch_current_co2_value, device_uuid)


# ------------------------------------------------------------------------------
# Get the current temp value for this device.
# Returns a float or None.
def get_current_temp_value(device_uuid):
    # First: look in the Datastore Device data dict...
    result = get_current_float_value_from_DS(DS_temp_KEY, device_uuid)
    if result is not None:
        return result

    # Second: do a big (slow) query
    return get_current_float_value_from_BQ(
        queries.fetch_current_temperature_value, device_uuid)


# ------------------------------------------------------------------------------
# Get the current RH value for this device.
# Returns a float or None.
def get_current_RH_value(device_uuid):
    # First: look in the Datastore Device data dict...
    result = get_current_float_value_from_DS(DS_rh_KEY, device_uuid)
    if result is not None:
        return result

    # Second: do a big (slow) query
    return get_current_float_value_from_BQ(
        queries.fetch_current_RH_value, device_uuid)
