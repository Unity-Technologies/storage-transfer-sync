#!/usr/bin/env python
#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import argparse
import datetime
import json

import googleapiclient.discovery


def main(description, project_id, kickoff_datetime, transfer_stop_datetime,
         elapsed_last_modification, source_bucket, sink_bucket, include_prefix):
    """Create a transfer from the Google Cloud Storage to Google Cloud Storage"""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    transfer_job = {
        'description': '{} fired at {}'.format(description, kickoff_datetime.strftime('%Y-%m-%dT%H:%M:%S+00:00')),
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': kickoff_datetime.day,
                'month': kickoff_datetime.month,
                'year': kickoff_datetime.year
            },
            'startTimeOfDay': {
                'hours': kickoff_datetime.hour,
                'minutes': kickoff_datetime.minute,
                'seconds': kickoff_datetime.second
            },
            "scheduleEndDate": {
                'day': transfer_stop_datetime.day,
                'month': transfer_stop_datetime.month,
                'year': transfer_stop_datetime.year
            }
        },
        'transferSpec': {
            'gcsDataSource': {
                'bucketName': source_bucket
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            },
            'transferOptions': {
                'deleteObjectsFromSourceAfterTransfer': 'false',
                'overwriteObjectsAlreadyExistingInSink': 'false'
            },
            'objectConditions': {
                'minTimeElapsedSinceLastModification': '{}s'.format(elapsed_last_modification),
                'includePrefixes': [ include_prefix ],
            }
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--description', help='Transfer description.')
    parser.add_argument('--project-id', help='Your Google Cloud project ID.')
    parser.add_argument('--kickoff-delay-minutes', help='Set the kickoff time delay in minutes.')
    parser.add_argument('--source-bucket', help='Source bucket name.')
    parser.add_argument('--sink-bucket', help='Sink bucket name.')
    parser.add_argument('--include-prefix', help='Include Prefix for the Transfer.')
    parser.add_argument('--transfer-stop-minutes', help='Minutes to Stop Transfer Job after Kickoff.')
    parser.add_argument('--elapsed-last-modification', help='Minimum Time Elapsed Seconds since Last Modification for the Sync.')


    args = parser.parse_args()
    now = datetime.datetime.utcnow()
    description = args.description
    project_id = args.project_id
    source_bucket = args.source_bucket
    sink_bucket = args.sink_bucket
    kickoff_delay_minutes = int(args.kickoff_delay_minutes)
    transfer_stop_minutes = int(args.transfer_stop_minutes)
    elapsed_last_modification = args.elapsed_last_modification
    kickoff_datetime = now + datetime.timedelta(minutes=kickoff_delay_minutes)
    include_prefix = args.include_prefix
    transfer_stop_datetime = kickoff_datetime + datetime.timedelta(minutes=transfer_stop_minutes)

    main(
        description,
        project_id,
        kickoff_datetime,
        transfer_stop_datetime,
        elapsed_last_modification,
        source_bucket,
        sink_bucket,
        include_prefix)
