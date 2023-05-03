# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# generate_network_report.py

import admob_utils
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
# import json
from flatten_json import flatten
import base64
import functions_framework
from datetime import datetime, date, timedelta
import pytz
import os
import sys


PUBLISHER_ID = os.environ.get('PUBLISHER_ID')


def generate_network_report(service, backfill=False):
    """Generates and prints a network report.

    Args:
      service: An AdMob Service Object.
      backfill: A Boolean flag to indicate whether to backfill data or not.
    """
    # Set date range. AdMob API only supports the account default timezone and
    # "America/Los_Angeles", see
    # https://developers.google.com/admob/api/v1/reference/rest/v1/accounts.networkReport/generate
    # for more information.
    tz = pytz.timezone('America/Los_Angeles')
    datetime_now = datetime.now(tz)

    # Set dimensions.
    dimensions = ['DATE', 'APP', 'COUNTRY']

    # Set metrics.
    metrics = ['ESTIMATED_EARNINGS', 'IMPRESSION_RPM', 'AD_REQUESTS', 'MATCH_RATE',
            'MATCHED_REQUESTS', 'SHOW_RATE', 'IMPRESSIONS', 'IMPRESSION_CTR', 'CLICKS']

    # Set sort conditions.
    sort_conditions = {'dimension': 'DATE', 'order': 'ASCENDING'}

    data = []

    if not backfill:
        end_date = datetime_now.date() - timedelta(days=1)
        date_range = {
            'start_date': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day},
            'end_date': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day}
        }

        print(date_range)

        # Create network report specifications.
        report_spec = {
            'date_range': date_range,
            'dimensions': dimensions,
            'metrics': metrics,
            'sort_conditions': [sort_conditions],
            'localization_settings': {'currency_code': 'USD'},
        }

        # Create network report request.
        request = {'report_spec': report_spec}

        # Execute network report request.
        response = service.accounts().networkReport().generate(
            parent='accounts/{}'.format(PUBLISHER_ID), body=request).execute()
        
        num_rows = int(response[-1]['footer']['matchingRowCount'])
        data = response[1:-1]
    else:
        end_date = datetime_now.date() - timedelta(days=1)

        # if the cloud scheduler is still going to run for yesterday at 02:00, then start the backfill from the day before yesterday (two days ago)
        cloud_scheduler_time = datetime(year=datetime_now.year, month=datetime_now.month, day=datetime_now.day, hour=2, minute=0, tzinfo=tz)
        if datetime_now < cloud_scheduler_time: 
            end_date = datetime_now.date() - timedelta(days=2)

        if (os.environ.get('START_DATE_YEAR') and os.environ.get('START_DATE_MONTH') and os.environ.get('START_DATE_DAY')):
            start_date = date(int('20' + os.environ.get('START_DATE_YEAR')), int(os.environ.get('START_DATE_MONTH')), int(os.environ.get('START_DATE_DAY')))
        else:
            return

        date_range = {
            'start_date': {'year': start_date.year, 'month': start_date.month, 'day': start_date.day},
            'end_date': {'year': end_date.year, 'month': end_date.month, 'day': end_date.day}
        }

        left, right = 1, (end_date - start_date).days + 1
        while left <= right:
            mid = (left + right) // 2
            end_date_mid = start_date + timedelta(days=mid - 1)

            date_range = {
                'start_date': {'year': start_date.year, 'month': start_date.month, 'day': start_date.day},
                'end_date': {'year': end_date_mid.year, 'month': end_date_mid.month, 'day': end_date_mid.day}
            }

            # Create network report specifications.
            report_spec = {
                'date_range': date_range,
                'dimensions': dimensions,
                'metrics': metrics,
                'sort_conditions': [sort_conditions],
                'localization_settings': {'currency_code': 'USD'},
            }

            # Create network report request.
            request = {'report_spec': report_spec}

            response = service.accounts().networkReport().generate(
                parent='accounts/{}'.format(PUBLISHER_ID), body=request).execute()

            # If the total number of records in the response is less than 100k,
            # try a larger batch size; otherwise, try a smaller batch size
            num_rows = int(response[-1]['footer']['matchingRowCount'])
            if num_rows < 100000:
                left = mid + 1
            else:
                right = mid - 1

        # The optimal batch size is the largest batch size that returns less than 100k rows
        batch_size = right
        print('batch_size: ', batch_size, '\n')

        # Loop through each batch of data, adjusting the date range as needed
        offset = 0
        while True:
            # Calculate the start and end dates for the current batch
            batch_start_date = start_date + timedelta(days=offset - 1)
            batch_end_date = min(batch_start_date + timedelta(days=batch_size - 1), end_date)
            
            # Make the API request for the current batch
            date_range = {
                'start_date': {'year': batch_start_date.year, 'month': batch_start_date.month, 'day': batch_start_date.day},
                'end_date': {'year': batch_end_date.year, 'month': batch_end_date.month, 'day': batch_end_date.day}
            }

            print(date_range)

            # Create network report specifications.
            report_spec = {
                'date_range': date_range,
                'dimensions': dimensions,
                'metrics': metrics,
                'sort_conditions': [sort_conditions],
                'localization_settings': {'currency_code': 'USD'},
            }

            # Create network report request.
            request = {'report_spec': report_spec}

            response = service.accounts().networkReport().generate(
                parent='accounts/{}'.format(PUBLISHER_ID), body=request).execute()

            num_rows = int(response[-1]['footer']['matchingRowCount'])
            print('batch num_rows: ', num_rows, '\n')

            # Append the data from the current batch to the overall data list
            data.extend(response[1:-1])
            
            # Increment the offset by the number of days in the current batch
            offset += batch_size
            
            # If the end date of the current batch is equal to the overall end date,
            # we have retrieved all the data and can break out of the loop
            if batch_end_date == end_date:
                break
        

    # Flatten the json and filter out empty country rows.
    idx = 0
    data_length = len(data)
    while idx < data_length:
        data[idx] = flatten(data[idx]['row'])
        if 'dimensionValues_COUNTRY' in data[idx]: # after flattening the data json, dimensionValues_COUNTRY will only exist if dimensionValues_COUNTRY_value does not exist            
            data[idx].pop('dimensionValues_COUNTRY')
            data[idx]['dimensionValues_COUNTRY_value'] = 'Unknown Region'
        
        idx += 1

    # # Test out by saving the data to a local json file.
    # json_string = json.dumps(data)
    # with open('json_data.json', 'w') as outfile:
    #     outfile.write(json_string)

    # Construct a BigQuery client object.
    # TODO(developer): Set project to the project ID where the BigQuery table is located.
    client = bigquery.Client(project=os.environ.get('PROJECT_ID'))

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = os.environ.get('TABLE_ID')

    # Check if table exists
    table_exists = False
    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        pass

    # Create table if it doesn't exist
    if not table_exists:
        table = bigquery.Table(table_id)
        table = client.create_table(table)
        print(f"Table {table.table_id} created.")
    else:
        print(f"Table {table_id} already exists.")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)

    job = client.load_table_from_json(
        data, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def admob_report_main(cloud_event):
    service = admob_utils.authenticate()
    pub_id_attr = None
    backfill_attr = None
    if "attributes" in cloud_event.data["message"] and cloud_event.data["message"]["attributes"]:
        attr_dic = cloud_event.data["message"]["attributes"]
        if "backfill" in attr_dic and attr_dic["backfill"]:
            backfill_attr = attr_dic["backfill"]
        if "pub_id" in attr_dic and attr_dic["pub_id"]:
            pub_id_attr = attr_dic["pub_id"]
    backfill = False
    if (backfill_attr == 'True' or backfill_attr == 'true' or backfill_attr == 'TRUE'):
        backfill = True
    if pub_id_attr and pub_id_attr != PUBLISHER_ID:
        return
    generate_network_report(service, backfill)


if __name__ == "__main__":
    service = admob_utils.authenticate()
    backfill = False
    if len(sys.argv) - 1:
        backfill = True
    generate_network_report(service, backfill)
