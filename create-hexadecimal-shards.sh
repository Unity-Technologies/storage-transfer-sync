#!/bin/bash

if [[ "$1" = '--max-jobs' ]]; then
    shift; max_jobs=$1; shift
else
    max_jobs=1000
fi

if [[ "$1" = '--name' ]]; then
    shift; job_name=$1; shift
fi

if [[ $# -lt 3 ]]; then
    cat <<EOF 2>&1
usage: $(basename "$0" .sh) [--max-jobs <count>] [--name <job_name>] \
<first_char> <last_char> <create_args>...

  Will submit at most ${max_jobs} jobs, each broken up with one or more prefixes to synchronize.

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
The final job will process ${last_batch_size} prefixes.
This will take at least $(( job_count / 60 )) minutes (rate limted at one submitted per second).

EOF

read -r -p 'Are you sure? [y/N] ' res
[[ "$res" = 'y' ]] || exit 2

set -e

last_hex=$(printf %x ${last})
fmt="%0${#last_hex}x" # same width as last char as hex

i=$first
while [[ $i -le $last ]]; do
    [[ $i -eq $last ]] && batch_size=${last_batch_size}
    prefixes=()
    b=0
    while [[ $b -lt ${batch_size} ]]; do
        prefixes+=("$(printf ${fmt} $i)")
        (( ++b ))
        (( ++i ))
    done
    desc="${job_name} ${prefixes[*]}"
    echo "--- ${desc} ----------------------------------------------------------------------"
    args=("$@" --include-prefix "${prefixes[@]}" --description "${desc}")
    echo ./create.py $(printf ' %q' "${args[@]}")
    ./create.py "${args[@]}"
    sleep 1 # avoid quota of "Maximum requests per 100 seconds per user: 100"
done
