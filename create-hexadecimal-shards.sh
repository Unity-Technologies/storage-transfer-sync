#!/bin/bash

max_jobs=1000

while true; do
    case "$1" in
        --max-jobs) shift; max_jobs=$1; shift;;
        --name) shift; job_name=$1; shift;;
        *) break
    esac
done

if [[ $# -lt 3 ]]; then
    cat <<EOF 2>&1
usage: $(basename "$0" .sh) [--max-jobs <count>] [--name <job_name>] \
<first_char> <last_char> <create_args>...

  Will submit at most ${max_jobs} jobs, each broken up with one or more prefixes to synchronize.

  https://cloud.google.com/storage-transfer/quotas

EOF
    exit 1
fi

first=$1; shift
last=$1; shift
count=$(( last - first + 1 ))

batch_size=$(( count / ${max_jobs} ))
[[ $(( count % ${max_jobs} )) -gt 0 ]] && (( ++batch_size ))

job_count=$(( count / batch_size ))
last_batch_size=$(( count % batch_size ))

cat <<EOF

This will submit ${job_count} jobs each processing ${batch_size} prefixes. \
The final job will process ${last_batch_size} prefixes. \
A total of ${count} prefixes will be processed.
This will take at least $(( job_count / 60 )) minutes (rate limted at one submitted per second).

EOF

read -r -p 'Are you sure? [y/N] ' res
[[ "$res" = 'y' ]] || exit 2

set -e

last_hex=$(printf %x ${last})
fmt="%0${#last_hex}x" # same width as last char as hex

failures=0
cnt=0
i=$first
while [[ $i -le $last ]]; do
    (( ++cnt ))
    [[ $i -eq $last ]] && batch_size=${last_batch_size}
    prefixes=()
    b=0
    while [[ $b -lt ${batch_size} ]]; do
        prefixes+=("$(printf ${fmt} $i)")
        (( ++b ))
        (( ++i ))
    done
    desc="${job_name} ${prefixes[*]}"
    echo "--- job #${cnt} (${failures} failed): ${desc} ----------------------------------------------------------------------"
    args=("$@" --include-prefix "${prefixes[@]}" --description "${desc}")
    echo ./create.py $(printf ' %q' "${args[@]}")
    if ! ./create.py "${args[@]}"; then
        rc=$?
        (( ++failures ))
        cat <<EOF >&2

****************************************
*** FAILED! Will attempt a retry in a few seconds...

EOF
        sleep 3
        ./create.py "${args[@]}"
    fi
    sleep 1 # avoid quota of "Maximum requests per 100 seconds per user: 100"
done
