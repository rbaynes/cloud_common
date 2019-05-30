# https://google-cloud-python.readthedocs.io/en/stable/bigquery/usage.html

# BigQuery is SLOW.  Only use it for research queries and warn the user.
# Note: most data from the device is cached in the datastore.

import ast
from google.cloud import bigquery

# This should be the only place we use queries.
from queries import queries

bigquery_client = bigquery.Client()


# ------------------------------------------------------------------------------
# Get a dict with two arrays of the temp and humidity historical values.
# Returns a dict.
def get_temp_and_humidity_history_from_BQ(device_uuid):
    humidity_array = []
    temp_array = []
    result_json = {
        'RH': humidity_array,
        'temp': temp_array
    }
    if device_uuid is None or device_uuid is 'None':
        return result_json

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_str = queries.formatQuery(
        queries.fetch_temp_results_history, device_uuid)
    query_job = bigquery_client.query(query_str, job_config=job_config)
    query_result = query_job.result()
    for row in list(query_result):
        rvalues = row[2]  # can't use row.values
        values_json = (ast.literal_eval(rvalues))

        if 'air_temperature_celcius' == row.var and 'values' in values_json:
            values = values_json["values"]
            result_json["temp"].append(
                {'value': values[0]['value'], 'time': row.eastern_time})

        if 'air_humidity_percent' == row.var and 'values' in values_json:
            values = values_json["values"]
            result_json["RH"].append(
                {'value': values[0]['value'], 'time': row.eastern_time})
    return result_json


