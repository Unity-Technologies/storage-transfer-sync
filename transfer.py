#!/usr/bin/env python

import argparse
import yaml
import dateutil.parser
from collections import defaultdict
from datetime import datetime

import googleapiclient.discovery


def main(project_id, filter_source, filter_sink, show_all_transfers, delete_jobs):
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    operations = defaultdict(list)
    for operation in each_operation(storagetransfer, project_id):
        if filtered(operation, filter_source, filter_sink):
            continue
        operations[operation['transferJobName']].append(operation)

    jobs = 0
    transfers = 0
    status = defaultdict(int)
    total = defaultdict(int)

    for job in each_job(storagetransfer, project_id):
        ops = operations.get(job['name'])
        if not ops:
            continue
        jobs += 1
        print('='*100)
        dump(job)
        operation = recent_operation(ops, show_all_transfers)
        transfers += len(ops)
        status[operation['status'].lower()] += 1
        counters = operation['counters']
        for (key, value) in counters.items():
            total[key] += int(value)
        if delete_jobs:
            delete_job(storagetransfer, project_id, job_name)

    print('='*100)
    print('Matched %d jobs and %d transfers: %s' % (
        jobs, transfers,
        ', '.join(['%d %s' % (c, k) for (k, c) in status.items()])
    ))

    if total['bytesFoundFromSource'] > 0:
        copied = total['bytesCopiedToSink']
        found = total['bytesFoundFromSource']
        print('Copied %4.1f%% bytes: %s / %s, %s failed, %s skipped' % (
            float(copied) / found * 100,
            sizeof_fmt(copied), sizeof_fmt(found),
            sizeof_fmt(total['bytesFromSourceFailed']),
            sizeof_fmt(total['bytesFromSourceSkippedBySync'])
        ))

    if total['objectsFoundFromSource'] > 0:
        copied = total['objectsCopiedToSink']
        found = total['objectsFoundFromSource']
        print('Found %4.1f%% objects: %s / %s, %s failed, %s skipped' % (
            float(copied) / found * 100,
            sizeof_fmt(copied, '', False), sizeof_fmt(found, '', False),
            sizeof_fmt(total['objectsFromSourceFailed'], '', False),
            sizeof_fmt(total['objectsFromSourceSkippedBySync'], '', False)
        ))

def each_operation(storagetransfer, project_id):
    request = storagetransfer.transferOperations().list(
        name='transferOperations',
        filter='{"project_id":"%s"}' % (project_id)
    )
    while request is not None:
        response = request.execute()
        for operation in response['operations']:
            yield operation['metadata']
        request = storagetransfer.transferOperations().list_next(
            previous_request=request,
            previous_response=response
        )

def each_job(storagetransfer, project_id):
    request = storagetransfer.transferJobs().list(filter='{"project_id":"%s"}' % (project_id))
    while request is not None:
        response = request.execute()
        for job in response['transferJobs']:
            yield job
        request = storagetransfer.transferJobs().list_next(
            previous_request=request,
            previous_response=response
        )

def recent_operation(operations, show_all):
    in_progress = False
    last_ts = None
    youngest = None
    for operation in operations:
        if not youngest or show_all:
            # drop keys already reported by the job
            del(operation['transferJobName'])
            del(operation['transferSpec'])
            print('-'*30)
            dump(operation)
        end = operation.get('endTime')
        if end:
            ts = dateutil.parser.parse(end)
            if not last_ts or ts > last_ts:
                last_ts = ts
                youngest = operation
        elif in_progress:
            raise ValueError('More than one transfer in progress')
        else:
            ts = dateutil.parser.parse(operation['startTime'])
            if not last_ts or ts > last_ts:
                last_ts = ts
                youngest = operation
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

def sizeof_fmt(num, suffix='B', binary=True):
    units = ['','K','M','G','T','P','E','Z','Y']
    if binary:
        base = 1024
        units = [u if u == '' else u+'i' for u in units]
    else:
        base = 1000
    for unit in units:
        if abs(num) < base:
            break
        num /= base
    if num == 0.0:
        return '0'+suffix
    return "%.1f%s%s" % (num, unit, suffix)

def dump(obj):
    print(yaml.safe_dump(obj))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project-id', help='Your Google Cloud project ID.')
    parser.add_argument('--filter-source-bucket', help='Show only matching source buckets')
    parser.add_argument('--filter-sink-bucket', help='Show only matching sink buckets')
    parser.add_argument('--show-all-transfers', action='store_true',
                        help='Show all transfers (default shows only most recent)')
    parser.add_argument('--delete', action='store_true', help='Delete all matching jobs')
    args = parser.parse_args()
    main(args.project_id, args.filter_source_bucket, args.filter_sink_bucket,
         args.show_all_transfers, args.delete)
