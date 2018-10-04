#!/bin/bash

if [[ $# -lt 4 ]]; then
    cat <<EOF >&2
usage: $(basename "$0") <gcp_project> <job_name> <interval_seconds> <create_args...>
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
    ./transfer.py "${GCP_PROJECT}" --filter-job-description "${JOB_NAME}" --delete
    log '*** DONE ***'
}

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

started_at=$(date +%s)
i=1
while true; do
    wait_for_complete
    echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'
    now=$(date +%s)
    diff=$(( $INTERVAL_SECONDS - ($now - $started_at) ))
    if [[ $diff -gt 0 ]]; then
        log "Waiting for ${diff} seconds..."
        sleep $diff
        log "done"
    fi
    args=("${CREATE_ARGS[@]}" --include-prefix $(recent_upids) --description "${JOB_NAME}-${i}")
    printf ' %q' "${args[@]}"; echo
    "${args[@]}"
    started_at=$(date +%s)
    (( ++i ))
done
