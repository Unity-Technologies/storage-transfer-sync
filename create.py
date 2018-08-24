#!/usr/bin/env python

import argparse
import yaml
from datetime import datetime, timedelta

import googleapiclient.discovery


def main(args):
    source_type, source_bucket = args.source_bucket.split('://', 1)
    if source_type == 's3':
        source_spec = create_s3_spec(
            source_bucket, args.aws_access_key_id, args.aws_secret_access_key, True)
    elif source_type == 'gs':
        source_spec = create_gcs_spec(source_bucket, True)
    else:
        raise ValueError('Unknown source type: ' + source_type)

    sink_type, sink_bucket = args.sink_bucket.split('://', 1)
    if sink_type == 's3':
        sink_spec = create_s3_spec(
            sink_bucket, args.aws_access_key_id, args.aws_secret_access_key, False)
    elif sink_type == 'gs':
        sink_spec = create_gcs_spec(sink_bucket, False)
    else:
        raise ValueError('Unknown sink type: ' + sink_type)

    now = datetime.utcnow()
    if args.schedule == 'daily':
        kickoff_datetime = args.start_time.replace(day=now.day, month=now.month, year=now.year)
        if kickoff_datetime < now:
            kickoff_datetime += timedelta(days=1) # already past, schedule for tomorrow
    else:
        kickoff_datetime = now + timedelta(minutes=args.minutes_from_now)

    prefixes = {}
    if args.include_prefix:
        prefixes['include'] = args.include_prefix
    if args.exclude_prefix:
        prefixes['exclude'] = args.exclude_prefix

    response = create(args.project_id, args.schedule == 'daily', kickoff_datetime, prefixes,
                      args.elapsed_last_modification, source_spec, sink_spec, args.description)
    dump(response)

def create(project_id, daily, kickoff_datetime, prefixes, elapsed_last_modification,
           source_spec, sink_spec, description, **transferOptions):
    """Create a storage transfer job"""

    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    schedule = {
        'scheduleStartDate': {
            'day': kickoff_datetime.day,
            'month': kickoff_datetime.month,
            'year': kickoff_datetime.year
        }
    }
    if daily:
        # only need a start day and time (no end)
        schedule['startTimeOfDay'] = {
            'hours': kickoff_datetime.hour,
            'minutes': kickoff_datetime.minute,
            'seconds': kickoff_datetime.second
        }
    else:
        # one-time job forces start and end on the same day
        schedule['scheduleEndDate'] = schedule['scheduleStartDate']

    conditions = {}
    if prefixes:
        if 'include' in prefixes:
            conditions['includePrefixes'] = prefixes['include']
        if 'exclude' in prefixes:
            conditions['excludePrefixes'] = prefixes['exclude']
    if elapsed_last_modification:
        conditions['minTimeElapsedSinceLastModification'] = '{}s'.format(elapsed_last_modification),

    transfer_spec = {
        'transferOptions': transferOptions,
        'objectConditions': conditions,
    }

    transfer_spec.update(source_spec)
    transfer_spec.update(sink_spec)

    transfer_job = {
        'description': '{} fired at {}'.format(
            description, kickoff_datetime.strftime('%Y-%m-%dT%H:%M:%S+00:00')),
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': schedule,
        'transferSpec': transfer_spec,
    }

    return storagetransfer.transferJobs().create(body=transfer_job).execute()

def create_s3_spec(bucket_name, access_key_id, secret_access_key, as_source):
    return create_data_spec(
        'awsS3',
        bucket_name,
        as_source,
        awsAccessKey={
            'accessKeyId': access_key_id,
            'secretAccessKey': secret_access_key
        }
    )

def create_gcs_spec(bucket_name, as_source):
    return create_data_spec('gcs', bucket_name, as_source)

def create_data_spec(data_type, bucket_name, as_source, **kwargs):
    key = data_type + 'Data' + ('Source' if as_source else 'Sink')
    spec = kwargs.copy()
    spec['bucketName'] = bucket_name
    return {key: spec}

def dump(obj):
    print(yaml.safe_dump(obj))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--description', default='', help='Transfer job description')
    parser.add_argument('--include-prefix', help='Include prefix for the transfer job')
    parser.add_argument('--exclude-prefix', help='Exclude prefix for the transfer job')
    parser.add_argument('--elapsed-last-modification', type=int,
                        help='Minimum elapsed seconds since the source objects was modified')
    parser.add_argument('--aws-access-key-id', help='AWS access key ID')
    parser.add_argument('--aws-secret-access-key', help='AWS secret access key')

    parser.add_argument('project_id', help='Your Google Cloud project ID')
    parser.add_argument('source_bucket',
                        help='Source bucket name (include gs:// or s3:// prefix)')
    parser.add_argument('sink_bucket',
                        help='Sink bucket name (include gs:// or s3:// prefix)')

    schedule_parser = parser.add_subparsers(title='schedule', dest='schedule')

    daily_parser = schedule_parser.add_parser('daily', help='Schedule a reoccuring transfer job')
    daily_parser.add_argument('start_time', type=lambda t: datetime.strptime(t, '%H:%M'),
                              help='Time to start transfer job each day')

    once_parser = schedule_parser.add_parser('once', help='Schedule a transfer job to run once')
    once_parser.add_argument('minutes_from_now', type=int,
                             help='Number of minutes from now to start the transfer job')

    main(parser.parse_args())
