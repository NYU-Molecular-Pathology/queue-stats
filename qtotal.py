#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Get the total resource usage from SLURM

similar to:

squeue -u $USER -o '%i|%T|%P|%m'
squeue -u $USER -O 'jobid,state,minmemory,mincpus'
"""
import util.slurm as slurm
import os
import re

username = os.environ.get('USER')

args = ['squeue', '-u', username, '-O', 'jobid,state,minmemory,mincpus']
queue = slurm.Squeue(command = args)

running = {}
running['entries'] = []
running['cpus'] = 0
running['mem'] = 0.0

pending = {}
pending['entries'] = []
pending['cpus'] = 0
pending['mem'] = 0.0

mem_key = {
'T': 1024 * 1024 * 1024 * 1024,
'G': 1024 * 1024 * 1024,
'M': 1024 * 1024,
'K': 1024
}

# need regex to strip the letters from the mem value
non_decimal = re.compile(r'[^\d.]+')

for entry in queue.entries:
    cpus = entry['MIN_CPUS']
    mem = entry['MIN_MEMORY']
    # convert to a float
    mem_num = float(non_decimal.sub('', mem))

    # parse the memory; 16T, "16G", "16M", "16K"
    if 'K' in mem:
        mem_val = mem_num * mem_key['K']
    elif 'M' in mem:
        mem_val = mem_num * mem_key['M']
    elif 'G' in mem:
        mem_val = mem_num * mem_key['G']
    elif 'T' in mem:
        mem_val = mem_num * mem_key['T']
    else:
        # silently drop non-matching values.. ?
        mem_val = 0

    if entry['STATE'] == 'PENDING':
        pending['entries'].append(entry)
        pending['cpus'] += int(cpus)
        pending['mem'] += mem_val
    elif entry['STATE'] == 'RUNNING':
        running['entries'].append(entry)
        running['cpus'] += int(cpus)
        running['mem'] += mem_val

print("Total usage: {cpus} CPUs, {mem}GB memory".format(cpus = running['cpus'], mem = running['mem'] / mem_key['G']))
