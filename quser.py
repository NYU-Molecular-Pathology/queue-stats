#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Get the SLURM cluster usage per user

- number of jobs running, pending
- number cpus allocated
- percent total usage
- total time requested for running jobs
- memory allocated, memory used

per-partition:
- #jobs|#cpus|alloc_mem
"""
import util.slurm as slurm
import re
import time
import datetime
import csv
import sys

def parse_time(time_str):
    """
    Parse a SLURM timestamp, return total seconds

    Examples
    --------
    >>> parse_time('2-14:01:37')
    223297.0

    >>> parse_time('18-02:02:46')
    1562566.0

    >>> parse_time('0:00:00')
    0.0

    >>> parse_time("0:02")
    2.0

    """
    # num seconds in a day
    seconds_per_day = 86400.0
    num_days = 0
    total_seconds = 0.0
    if '-' in time_str:
        parts = time_str.split('-')
        num_days += int(parts[0])
        time_str_split = parts[1]
        x = time.strptime(time_str_split, '%H:%M:%S')
        total_seconds += datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
        total_seconds += num_days * seconds_per_day
    elif time_str.count(':') == 1:
        x = time.strptime(time_str, '%M:%S')
        total_seconds += datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
    else:
        x = time.strptime(time_str, '%H:%M:%S')
        total_seconds += datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
    return(total_seconds)

mem_key = {
'T': 1024 * 1024 * 1024 * 1024,
'G': 1024 * 1024 * 1024,
'M': 1024 * 1024,
'K': 1024
}

# need regex to strip the letters from the mem value
non_decimal = re.compile(r'[^\d.]+')

def parse_mem(mem_str, mem_key = mem_key, non_decimal = non_decimal):
    """
    Parse SLURM memory values, return total in bytes
    """
    # convert to a float
    mem_num = float(non_decimal.sub('', mem_str))

    # parse the memory; 16T, "16G", "16M", "16K"
    if 'K' in mem_str:
        mem_val = mem_num * mem_key['K']
    elif 'M' in mem_str:
        mem_val = mem_num * mem_key['M']
    elif 'G' in mem_str:
        mem_val = mem_num * mem_key['G']
    elif 'T' in mem_str:
        mem_val = mem_num * mem_key['T']
    else:
        # silently drop non-matching values.. ?
        mem_val = 0
    return(mem_val)

# get the SLURM squeue
# USER JOBID PARTITION STATE MIN_MEMORY MIN_CPUS TIME TIME_LIMIT
args = ['squeue', '-O', 'username,jobid,partition,state,minmemory,mincpus,timeused,timelimit']
queue = slurm.Squeue(command = args)

# dict to hold per-user entries
users = {}

# parse the squeue entries
for entry in queue.entries:
    username = entry['USER']
    partition = entry['PARTITION']
    state = entry['STATE']
    mem = entry['MIN_MEMORY']
    cpus = entry['MIN_CPUS']
    time_used = entry['TIME']
    time_limit = entry['TIME_LIMIT']

    # parse values
    time_used_val = parse_time(time_used)
    mem_val = parse_mem(mem)

    if state == "RUNNING":
        # initialize user dict
        if username not in users:
            users[username] = {}

        # dict for user's total running stats per partition
        # initialize defaults if they are not already present
        if 'partitions' not in users[username]:
            users[username]['partitions'] = {}
        if partition not in users[username]['partitions']:
            users[username]['partitions'][partition] = {}
        if 'jobs' not in users[username]['partitions'][partition]:
            users[username]['partitions'][partition]['jobs'] = 0
        if 'cpus' not in users[username]['partitions'][partition]:
            users[username]['partitions'][partition]['cpus'] = 0
        if 'mem' not in users[username]['partitions'][partition]:
            users[username]['partitions'][partition]['mem'] = 0.0
        if 'time' not in users[username]['partitions'][partition]:
            users[username]['partitions'][partition]['time'] = 0.0

        # add the new values
        users[username]['partitions'][partition]['cpus'] += int(cpus)
        users[username]['partitions'][partition]['mem'] += mem_val
        users[username]['partitions'][partition]['time'] += time_used_val
        users[username]['partitions'][partition]['jobs'] += 1

        # dict for total usage
        if 'total' not in users[username]:
            users[username]['total'] = {}
        if 'cpus' not in users[username]['total']:
            users[username]['total']['cpus'] = 0
        if 'mem' not in users[username]['total']:
            users[username]['total']['mem'] = 0.0
        if 'time' not in users[username]['total']:
            users[username]['total']['time'] = 0.0
        if 'jobs' not in users[username]['total']:
            users[username]['total']['jobs'] = 0

        users[username]['total']['cpus'] += int(cpus)
        users[username]['total']['mem'] += mem_val
        users[username]['total']['time'] += time_used_val
        users[username]['total']['jobs'] += 1

    elif state == "PENDING":
        # initialize user dict
        if username not in users:
            users[username] = {}

        # initialize dict for pending jobs
        if 'pending' not in users[username]:
            users[username]['pending'] = {}
        if 'jobs' not in users[username]['pending']:
            users[username]['pending']['jobs'] = 0
        if 'cpus' not in users[username]['pending']:
            users[username]['pending']['cpus'] = 0
        if 'mem' not in users[username]['pending']:
            users[username]['pending']['mem'] = 0.0
        if 'time' not in users[username]['pending']:
            users[username]['pending']['time'] = 0.0

        users[username]['pending']['cpus'] += int(cpus)
        users[username]['pending']['mem'] += mem_val
        users[username]['pending']['time'] += time_used_val
        users[username]['pending']['jobs'] += 1


# print a table of the per-user metrics
partitions = []
for username, values in users.items():
    if 'partitions' in values:
        for key in values['partitions'].keys():
            partitions.append(key)

partitions = sorted(list(set(partitions)))
fieldnames = ['user']
for partition in partitions:
    fieldnames.append(partition)
fieldnames.append('total')
fieldnames.append('pending')


# print to console
writer = csv.DictWriter(sys.stdout, delimiter = '\t', fieldnames = fieldnames)
writer.writeheader()
for user, values in users.items():
    d = {}
    d['user'] = user

    for partition in partitions:
        if 'partitions' in values:
            if partition in values['partitions']:
                d[partition] = "{jobs}|{cpus}|{mem}G|{time:.1f}hr".format( #
                jobs = values['partitions'][partition]['jobs'],
                cpus = values['partitions'][partition]['cpus'],
                mem = values['partitions'][partition]['mem'] / mem_key['G'],
                time = values['partitions'][partition]['time'] / 3600,
                )
        else:
            d[partition] = ''

    if 'pending' in values:
        d['pending'] = "{jobs}|{cpus}|{mem}G|{time:.1f}hr".format(
        jobs = values['pending']['jobs'],
        cpus = values['pending']['cpus'],
        mem = values['pending']['mem'] / mem_key['G'],
        time = values['pending']['time'] / 3600,
        )
    else:
        d['pending'] = ''

    if 'total' in values:
        d['total'] = "{jobs}|{cpus}|{mem}G|{time:.1f}hr".format(
        jobs = values['total']['jobs'],
        cpus = values['total']['cpus'],
        mem = values['total']['mem'] / mem_key['G'],
        time = values['total']['time'] / 3600,
        )
    else:
        d['total'] = ''


    writer.writerow(d)
