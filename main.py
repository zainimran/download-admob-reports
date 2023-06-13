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
import json
from flatten_json import flatten
import functions_framework
from datetime import datetime, timedelta
import pytz
import os
import sys
import time
import logging


PUBLISHER_ID = os.environ.get('PUBLISHER_ID')


def list_apps(service, dry_run=False):
    """Lists all apps under an AdMob account.

    Args:
        service: An AdMob Service Object.
    """

    data = []
    next_page_token = ''

    while True:
        # Execute the request.
        response = service.accounts().apps().list(
            pageSize=100,
            pageToken=next_page_token,
            parent='accounts/{}'.format(PUBLISHER_ID)).execute()

        # Check if the response is empty.
        if not response:
            break

        # Print the result.
        apps = response['apps']
        for app in apps:
            if 'linkedAppInfo' in app and app['platform'] == 'ANDROID':
                data.append({
                    'app_id': app['appId'],
                    'app_store_id': app['linkedAppInfo']['appStoreId'],
                    'app_store_display_name': app['linkedAppInfo']['displayName'] if 'displayName' in app['linkedAppInfo'] else None,
                })

        if 'nextPageToken' not in response:
            break

        # Update the next page token.
        next_page_token = response['nextPageToken']
    
    if dry_run:
        try:
            # Test out by saving the data to a local json file.
            json_string = json.dumps(data)
            with open('apps_list.json', 'w') as outfile:
                outfile.write(json_string)
            print("dry run complete (local machine)")
        except Exception:
            print("dry run complete (cloud function)")
    else:
        client = bigquery.Client(project=os.environ.get('PROJECT_ID'))

        table_id = f"{os.environ.get('PROJECT_ID')}.admob_reporting_data.list_apps"

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
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

        job = client.load_table_from_json(
            data, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "{} rows and {} columns in {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )


def generate_network_report(service, backfill=False, dry_run=False, start_date_year='2022', start_date_month='1', start_date_day='1', end_date_year=None, end_date_month=None, end_date_day=None):
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
        if not end_date_year:
            end_date = datetime_now.date() - timedelta(days=1)
        else:
            end_date = datetime(year=int(end_date_year), month=int(end_date_month), day=int(end_date_day)).date()

        # if the cloud scheduler is still going to run for yesterday at 02:00, then start the backfill from the day before yesterday (two days ago)
        cloud_scheduler_time = datetime(year=datetime_now.year, month=datetime_now.month, day=datetime_now.day, hour=2, minute=0, tzinfo=tz)
        if datetime_now < cloud_scheduler_time and not end_date_year:
            end_date = datetime_now.date() - timedelta(days=2)

        if (start_date_year and start_date_month and start_date_day):
            start_date = datetime(year=int(start_date_year), month=int(start_date_month), day=int(start_date_day)).date()
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

            # If the total number of records in the response is less than 80k (record retrieval limit is 100k), try a larger batch size; otherwise, try a smaller batch size
            num_rows = int(response[-1]['footer']['matchingRowCount'])
            if num_rows < 80000:
                left = mid + 1
            else:
                right = mid - 1

        # The optimal batch size is the largest batch size that returns less than 80k rows 
        batch_size = right
        print('batch_size: ', batch_size, '\n')

        # Loop through each batch of data, adjusting the date range as needed
        offset = 0
        while True:
            # Calculate the start and end dates for the current batch
            batch_start_date = start_date + timedelta(days=max(offset, 0))
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
            if num_rows > 100000:
                logging.warning('Report not fully retrieved from the AdMob API due to number of rows in response exceeding 100k')
                batch_size -= 2 # decrease batch size aggresively until we don't hit the record retrieval limit on the API
                print('new batch_size: ', batch_size, '\n')
                continue

            # Append the data from the current batch to the overall data list
            data.extend(response[1:-1])
            
            # Increment the offset by the number of days in the current batch
            offset += batch_size
            
            # If the end date of the current batch is equal to the overall end date, we have retrieved all the data and can break out of the loop
            if batch_end_date == end_date:
                break
        

    # Flatten the json and map empty countries to 'Unknown Region' and format date in yyyy-mm-dd format.
    idx = 0
    data_length = len(data)
    while idx < data_length:
        data[idx] = flatten(data[idx]['row'])
        if 'dimensionValues_COUNTRY' in data[idx]: # after flattening the data json, dimensionValues_COUNTRY will only exist if dimensionValues_COUNTRY_value does not exist            
            data[idx].pop('dimensionValues_COUNTRY')
            data[idx]['dimensionValues_COUNTRY_value'] = 'Unknown Region'
        if 'dimensionValues_DATE_value' in data[idx]:
            date_string = data[idx]['dimensionValues_DATE_value']
            year, month, day = int(date_string[:4]), int(date_string[4:6]), int(date_string[6:])
            date = datetime(year, month, day)
            data[idx]['dimensionValues_DATE_value'] = date.strftime("%Y-%m-%d")
        idx += 1

    if dry_run:
        try:
            # Test out by saving the data to a local json file.
            json_string = json.dumps(data)
            with open('admob_reports.json', 'w') as outfile:
                outfile.write(json_string)
            print("dry run complete (local machine)")
        except Exception:
            print("dry run complete (cloud function)")
    else:
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
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

        # max retries for load job = 5
        for i in range(5):
            try:
                job = client.load_table_from_json(
                    data, table_id, job_config=job_config)
                print('\njob id: ', job.job_id, '\n')
                job.result()  # Waits for the job to complete.
                break
            except Exception as e:
                print(f"Error loading job: {e}")
                time.sleep(10 * (2 ** i)) # retry delay = 10

        table = client.get_table(table_id)  # Make an API request.
        print(
            "{} rows and {} columns in {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def admob_report_main(cloud_event):
    service = admob_utils.authenticate()
    pub_id = None
    backfill = False
    dry_run = False
    start_date_year = '2022'
    start_date_month = '1'
    start_date_day = '1'
    end_date_year = None
    end_date_month = None
    end_date_day = None
    populate_apps_list = None
    if "attributes" in cloud_event.data["message"] and cloud_event.data["message"]["attributes"]:
        attr_dic = cloud_event.data["message"]["attributes"]
        if "backfill" in attr_dic and attr_dic["backfill"]:
            backfill = attr_dic["backfill"]
        if "dry_run" in attr_dic and attr_dic["dry_run"]:
            dry_run = attr_dic["dry_run"]
        if "populate_apps_list" in attr_dic and attr_dic["populate_apps_list"]:
            populate_apps_list = attr_dic["populate_apps_list"]
        if "pub_id" in attr_dic and attr_dic["pub_id"]:
            pub_id = attr_dic["pub_id"]
        if "start_date_year" in attr_dic and attr_dic["start_date_year"]:
            start_date_year = attr_dic["start_date_year"]
        if "start_date_month" in attr_dic and attr_dic["start_date_month"]:
            start_date_month = attr_dic["start_date_month"]
        if "start_date_day" in attr_dic and attr_dic["start_date_day"]:
            start_date_day = attr_dic["start_date_day"]
        if "end_date_year" in attr_dic and attr_dic["end_date_year"]:
            end_date_year = attr_dic["end_date_year"]
        if "end_date_month" in attr_dic and attr_dic["end_date_month"]:
            end_date_month = attr_dic["end_date_month"]
        if "end_date_day" in attr_dic and attr_dic["end_date_day"]:
            end_date_day = attr_dic["end_date_day"]
    if pub_id and pub_id != PUBLISHER_ID:
        return
    if (backfill == 'True' or backfill == 'true' or backfill == 'TRUE'):
        backfill = True
    if (populate_apps_list == 'True' or populate_apps_list == 'true' or populate_apps_list == 'TRUE'):
        list_apps(service, dry_run)
    generate_network_report(service, backfill, dry_run, start_date_year, start_date_month, start_date_day, end_date_year, end_date_month, end_date_day)


if __name__ == "__main__":
    service = admob_utils.authenticate()
    backfill = False
    dry_run = True
    start_date_year = '2022'
    start_date_month = '1'
    start_date_day = '1'
    end_date_year = None
    end_date_month = None
    end_date_day = None
    if len(sys.argv) > 1 and (sys.argv[1] == '--backfill=true' or sys.argv[1] == '--backfill=custom'):
        backfill = True
    if len(sys.argv) > 1 and '--apps-list=true' in sys.argv:
        list_apps(service, dry_run)
    if "START_DATE_YEAR" in os.environ:
        start_date_year = os.environ.get('START_DATE_YEAR')
        start_date_month = os.environ.get('START_DATE_MONTH')
        start_date_day = os.environ.get('START_DATE_DAY')
    if len(sys.argv) > 1 and sys.argv[1] == '--backfill=custom':
        end_date_year = os.environ.get('END_DATE_YEAR')
        end_date_month = os.environ.get('END_DATE_MONTH')
        end_date_day = os.environ.get('END_DATE_DAY')
    generate_network_report(service, backfill, dry_run, start_date_year, start_date_month, start_date_day, end_date_year, end_date_month, end_date_day)
