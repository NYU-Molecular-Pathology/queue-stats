#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Check the HPC cluster to determine the following:

- partition with the most idle nodes
- partition with the most mixed nodes
- number of cpus used & requested on clinical queue 'intellispace'
- recommended best queue to submit to

Outputs to JSON file

Run this with a 'cron' entry for automated updating at regular intervals

Direct programs to read this JSON output in order to reduce 'sinfo' and 'squeue'
queries on the system
"""
import util.slurm as slurm
import datetime
import json

timestamp = datetime.datetime.now()
timestamp_str = timestamp.strftime('%Y-%m-%dT%H:%M:%S')

# output JSON file
output_json_file = "slurm.json"

# default best queue
best_queue = "intellispace"

# starting value of intellispace cpu usage
intellispace_cpus = 0

# limit on allowed intellispace queue usage
# allow more requested CPUs than true number to help bias 'best_queue' towards this queue more
intellispace_cpus_limit = 160

# do not submit to these queues
partition_blacklist = [
"data_mover",
"cpu_dev",
"gpu4_dev",
"gpu8_dev",
# "fn_short",
# "fn_medium",
"fn_long"
# "cpu_short",
# "cpu_medium"
#"cpu_long"
]

# get the partitions and jobs info from the system
partitions = slurm.Partitions()
squeue = slurm.Squeue()

def get_best_queue(
    partitions = partitions,
    squeue = squeue,
    intellispace_cpus = intellispace_cpus,
    intellispace_cpus_limit = intellispace_cpus_limit,
    partition_blacklist = partition_blacklist,
    timestamp_str = timestamp_str,
    output_json_file = output_json_file,
    best_queue = best_queue
    ):
    most_idle = partitions.most_idle_nodes(blacklist = partition_blacklist)
    most_mixed = partitions.most_mixed_nodes(blacklist = partition_blacklist)

    # count the number of intellispace cpus used
    for entry in squeue.entries:
        if entry['PARTITION'] == 'intellispace':
            intellispace_cpus += int(entry['CPUS'])

    # decide which queue is the best one to submit to based on current conditions;
    # 1. Use intellispace if it has openings
    # 2. Use the queue with the most idle nodes
    # 3. Use the queue with the most mixed nodes (some free CPUs)
    if intellispace_cpus < intellispace_cpus_limit:
        best_queue = 'intellispace'
    elif most_idle:
        best_queue = most_idle
    elif most_mixed:
        best_queue = most_mixed
    else:
        pass

    # print stdout message
    print("[{timestamp}] best queue: {best_queue}, most idle: {most_idle}, most mixed: {most_mixed}, intellispace cpus: {intellispace_cpus}".format(
    timestamp = timestamp_str,
    best_queue = best_queue,
    most_idle = most_idle,
    most_mixed = most_mixed,
    intellispace_cpus = intellispace_cpus
    ))

    # dict to hold the values to output
    data = {
    'updated': timestamp_str,
    'most_idle': most_idle,
    'most_mixed': most_mixed,
    'intellispace_cpus': intellispace_cpus,
    'best_queue': best_queue
    }

    # save the output JSON; overwrite old contents with new
    with open(output_json_file, 'w') as f:
        json.dump(data, f, indent = 4)


if partitions.sinfo.returncode == 0 and squeue.returncode == 0:
    get_best_queue()
else:
    print("[{timestamp}] error: SLURM command returned invalid exit status; sinfo code: {sinfo_code}, squeue code: {squeue_code}".format(
    timestamp = timestamp_str,
    sinfo_code = partitions.sinfo.returncode,
    squeue_code = squeue.returncode
    ))
