#!/bin/bash

if [[ "$1" = '--gsutil' ]]; then
    gsutil=true; shift
else
    gsutil=false
fi

if [[ $# -lt 4 ]]; then
    cat <<EOF >&2
usage: $(basename "$0") [--gsutil] <gcp_project> <job_name> <interval_seconds> <create_args...>
EOF
    exit 1
fi

if [[ -z "${MYSQL_HOST}" || -z "${MYSQL_PWD}" ]]; then
    echo 'Must set MYSQL_HOST and MYSQL_PWD (password) environment variables' >&2
    exit 2
fi

GCP_PROJECT=$1; shift
JOB_NAME=$1; shift
INTERVAL_SECONDS=$1; shift
CREATE_ARGS=(./create.py once "${GCP_PROJECT}" "$@")

DB_NAME=unity_cloud_collab

# make sure to always look back _PAST_ the last time mysql is queried by an additional 5 minutes
OVERLAP_SECONDS=300

if $gsutil; then
    function wait_for_complete()
    {
        local summary now started_at=$(date +%s)
        while true; do
            echo 'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'
            log
            summary="$(pgrep -laf 'gsutil rsync' || :)"
            echo "${summary}"
            now=$(date +%s)
            log "elapsedSeconds: $(( $now - $started_at ))"
            [[ -n "${summary}" ]] || break
            sleep 60
        done
        log '*** DONE ***'
    }

    S3_BUCKET=$1
    GS_BUCKET=$2

    [[ -d gsutil_logs ]] || mkdir gsutil_logs

    function sync()
    {
        local upid upids=($(recent_upids))
        log "SYNCING ${#upids[@]} UPIDS"
        for upid in ${upids[*]}; do
            gsutil rsync -Cr "${GS_BUCKET}/${upid}/" "${S3_BUCKET}/${upid}/" \
                   >"gsutil_logs/${1}-${upid}.log" 2>&1 &
        done
    }
else
    function wait_for_complete()
    {
        local in_progressCount summary
        while true; do
            echo 'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'
            log
            in_progressCount=''
            summary="$(./transfer.py "${GCP_PROJECT}" --filter-transfer-status in_progress --summarize shell)"
            if [[ $? -ne 0 ]]; then
                echo "${summary}"
                exit 2
            fi
            echo "${summary}"
            eval "${summary}"
            [[ $in_progressCount -gt 0 ]] || break
            sleep 60
        done
        log '*** DELETING ***'
        ./transfer.py "${GCP_PROJECT}" --filter-transfer-status success --filter-job-description "${JOB_NAME}" --delete
        log '*** DONE ***'
    }

    function sync()
    {
        local upids=($(recent_upids))
        local args=("${CREATE_ARGS[@]}" --include-prefix ${upids[*]} --description "$1")
        log "SYNCING ${#upids[@]} UPIDS"
        printf ' %q' "${args[@]}"; echo
        "${args[@]}"
    }
fi

LAST_QUERIED_AT=$(( $(date +%s) - $INTERVAL_SECONDS ))
function recent_upids()
{
    local mysql_ts=$(date --date "@$(( $LAST_QUERIED_AT - $OVERLAP_SECONDS ))" +'%Y-%m-%d %H:%M:%S')
    log "selecting upids updated since ${mysql_ts}"
    LAST_QUERIED_AT=$(date +%s)
    mysql_batch "SELECT project_fk FROM repos WHERE updated_at > \"${mysql_ts}\" LIMIT 1000" | \
        tr $'\n' ' '
}

function mysql_batch()
{
    mysql -u collab -D "${DB_NAME}" -NB -e "$@"
}

function log()
{
    echo "[$(date '+%Y-%m-%dT%H:%M:%S%z')] $*" >&2
}

set -e
set -o pipefail

log '*** STARTING ***'
started_at=0 # start immediately after waiting on any in progress the first time through
i=1
while true; do
    wait_for_complete
    echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'
    now=$(date +%s)
    diff=$(( $INTERVAL_SECONDS - ($now - $LAST_QUERIED_AT) ))
    if [[ $diff -gt 0 ]]; then
        log "Waiting for ${diff} seconds..."
        sleep $diff
        log "done"
    fi
    sync "${JOB_NAME}-${i}"
    started_at=$(date +%s)
    (( ++i ))
done
