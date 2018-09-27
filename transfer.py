#!/usr/bin/env python
"""
List Google Cloud Storage (GCS) transfer jobs with option to delete.
"""

import argparse
import json
import yaml
import time
import dateutil.parser
from collections import defaultdict
from datetime import datetime
from dateutil.tz import tzutc

import googleapiclient.discovery


def main(project_id, filter_job_status, filter_transfer_status, filter_source, filter_sink,
         filter_start, show_all_transfers, delete_jobs, summarize):

    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    operations = defaultdict(list)
    for operation in each_operation(storagetransfer, project_id, filter_transfer_status):
        if filtered(operation, filter_source, filter_sink):
            continue
        operations[operation['transferJobName']].append(operation)

    jobs = 0
    transfers = 0
    status = defaultdict(int)
    total = defaultdict(int)
    times = {'start': None, 'end': None}

    for job in each_job(storagetransfer, project_id, filter_job_status):
        job_name = job['name']
        ops = operations.get(job_name)
        if not ops:
            continue
        jobs += 1
        if not summarize:
            print('='*100)
            dump(job)
        operation = recent_operation(ops, filter_start, show_all_transfers, summarize)
        update_elapsed(times, operation)
        transfers += len(ops)
        status[operation['status'].lower()] += 1
        counters = operation['counters']
        for (key, value) in counters.items():
            total[key] += int(value)
        if delete_jobs:
            delete_job(storagetransfer, project_id, job_name)
            time.sleep(1)  # avoid quota of "Maximum requests per 100 seconds per user: 100"

    if times['start']:
        now = datetime.utcnow().replace(tzinfo=tzutc())
        if not times['end']:
            times['end'] = now
        total['elapsedSeconds'] = (times['end'] - times['start']).total_seconds()
        total['endHoursAgo'] = (now - times['end']).total_seconds() / 3600.0

    if summarize:
        total['jobCount'] = jobs
        total['transfer Count'] = transfers
        for k, c in status.items():
            total[k + 'Count'] = c
        if summarize == 'json':
            print(json.dumps(total))
        else:
            for k in sorted(total):
                v = total[k]
                if isinstance(v, int):
                    fmt = 'd'
                elif isinstance(v, float):
                    fmt = '0.1f'
                else:
                    fmt = 's'
                print(('%s=%'+fmt) % (k, v))
        return

    print('='*100)
    print('Matched %d jobs and %d transfers: %s' % (
        jobs, transfers,
        ', '.join(['%d %s' % (c, k) for (k, c) in status.items()])))
    if 'elapsedSeconds' in total:
        print('Ran for %0.1f seconds, finishing at %s, %0.1f hours ago' %(
            total['elapsedSeconds'], times['end'], total['endHoursAgo']))
        print('Oldest:')
        dump(times['oldestStartTransfer'])
        dump(times['oldestEndTransfer'])

    if total['bytesFoundFromSource'] > 0:
        copied = total['bytesCopiedToSink']
        found = total['bytesFoundFromSource']
        print('Copied %4.1f%% bytes: %s / %s, %s failed, %s skipped' % (
            float(copied) / found * 100,
            sizeof_fmt(copied), sizeof_fmt(found),
            sizeof_fmt(total['bytesFromSourceFailed']),
            sizeof_fmt(total['bytesFromSourceSkippedBySync'])))

    if total['objectsFoundFromSource'] > 0:
        copied = total['objectsCopiedToSink']
        found = total['objectsFoundFromSource']
        print('Found %4.1f%% objects: %s / %s, %s failed, %s skipped' % (
            float(copied) / found * 100,
            sizeof_fmt(copied, '', False), sizeof_fmt(found, '', False),
            sizeof_fmt(total['objectsFromSourceFailed'], '', False),
            sizeof_fmt(total['objectsFromSourceSkippedBySync'], '', False)))

def update_elapsed(times, operation):
    # find the oldest start and the newest end times
    start = operation.get('startTime')
    if start:
        start = dateutil.parser.parse(start)
        if times['start'] is None or start < times['start']:
            times['start'] = start
            times['oldestStartTransfer'] = operation
    end = operation.get('endTime')
    if end:
        end = dateutil.parser.parse(end)
        if times['end'] is None or end > times['end']:
            times['end'] = end
            times['oldestEndTransfer'] = operation

def each_operation(storagetransfer, project_id, *statuses):
    for operation in each_resource(storagetransfer.transferOperations, 'operations', project_id,
                                   'transfer_statuses', statuses, name='transferOperations'):
        yield operation['metadata']

def each_job(storagetransfer, project_id, *statuses):
    for job in each_resource(storagetransfer.transferJobs, 'transferJobs', project_id,
                             'job_statuses', statuses):
        yield job

def each_resource(func, resource_key, project_id, status_key, statuses, **list_kwargs):
    fltr = {'project_id': project_id}
    statuses = [s for s in statuses if s is not None]
    if statuses:
        fltr[status_key] = statuses
    list_kwargs['filter'] = json.dumps(fltr)
    request = func().list(**list_kwargs)
    while request is not None:
        response = request.execute()
        if resource_key not in response:
            return
        for resource in response[resource_key]:
            yield resource
        request = func().list_next(previous_request=request, previous_response=response)

def recent_operation(operations, start_day, show_all, summarize):
    in_progress = False
    last_ts = None
    youngest = None

    for operation in operations:
        start_ts = dateutil.parser.parse(operation['startTime'])
        if start_day and start_day != start_ts.day:
            continue

        end = operation.get('endTime')
        if end:
            end_ts = dateutil.parser.parse(end)
            operation['elapsedSeconds'] = (end_ts - start_ts).total_seconds()
            if not last_ts or end_ts > last_ts:
                last_ts = end_ts
                youngest = operation
        elif in_progress:
            raise ValueError('More than one transfer in progress')
        else:
            if not last_ts or start_ts > last_ts:
                last_ts = start_ts
                youngest = operation

        if show_all:
            # drop keys already reported by the job
            del(operation['transferJobName'])
            del(operation['transferSpec'])
            if not summarize:
                print('-'*30)
                dump(operation)

    if not summarize and not show_all:
        print('-'*30)
        dump(youngest)

    return youngest

def delete_job(storagetransfer, project_id, job_name):
    update_transfer_job = {
        'projectId': project_id,
        'updateTransferJobFieldMask': 'status',
        'transferJob': {'status': 'DELETED'},
    }
    request = storagetransfer.transferJobs().patch(jobName=job_name, body=update_transfer_job)
    response = request.execute()
    dump(response)

def filtered(resource, source, sink):
    spec = resource['transferSpec']
    return (
        (source and spec.get('gcsDataSource', spec.get('awsS3DataSource', {})).get('bucketName') != source)
        or
        (sink and spec.get('gcsDataSink', spec.get('awsS3DataSink', {})).get('bucketName') != sink)
    )

UNITS = ['','K','M','G','T','P','E','Z','Y']
BINARY_UNITS = [u if u == '' else u+'i' for u in UNITS]

def sizeof_fmt(num, suffix='B', binary=True):
    if binary:
        base = 1024
        units = BINARY_UNITS
    else:
        base = 1000
        units = UNITS
    minimum = base * 1.5 # show some granularity around the exact base boundary
    for unit in units:
        if num < minimum:
            break
        num /= base
    if num == 0.0:
        return '0'+suffix
    return "%.2f%s%s" % (num, unit, suffix)

def dump(obj):
    print(yaml.safe_dump(obj))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--filter-job-status', help='Show only jobs with matching status')
    parser.add_argument('--filter-transfer-status',
                        help='Show only jobs with matching transfer status')
    parser.add_argument('--filter-source-bucket', help='Show only matching source buckets')
    parser.add_argument('--filter-sink-bucket', help='Show only matching sink buckets')
    parser.add_argument('--filter-start-day', type=int, choices=range(1, 31),
                        help='Show only matching transfer on this day (UTC)')
    parser.add_argument('--show-all-transfers', action='store_true',
                        help='Show all transfers (default shows only most recent)')
    parser.add_argument('--delete', action='store_true', help='Delete all matching jobs')
    parser.add_argument('--summarize', choices=['json', 'shell'], help='Show only summary')
    parser.add_argument('project_id', help='Your Google Cloud project ID.')
    args = parser.parse_args()
    main(args.project_id, args.filter_job_status, args.filter_transfer_status,
         args.filter_source_bucket, args.filter_sink_bucket, args.filter_start_day,
         args.show_all_transfers, args.delete, args.summarize)
