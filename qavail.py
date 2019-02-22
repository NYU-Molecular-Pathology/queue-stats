#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Get SLURM availability summary

roughly analagous to:
sinfo -O 'allocmem,freemem,nodehost,partition'

>>> args = ['sinfo', '-O', 'allocmem,freemem,nodehost,partition,cpus,sockets,memory,gres,cpusstate,reason,statelong']
>>> n = slurm.Nodes(command = args)
"""
import util.slurm as slurm
import csv
import sys
# need extra args to get allocmem in the output
args = ['sinfo', '-O', 'allocmem,freemem,nodehost,partition,cpus,sockets,memory,gres,cpusstate,reason,statelong']
n = slurm.Nodes(command = args)

# fieldnames = n.avail[0].keys()
fieldnames = ['node', 'allocmem', 'freemem', 'pcnt_mem', 'cpu', 'state', 'partitions']

writer = csv.DictWriter(sys.stdout, delimiter = '\t', fieldnames = fieldnames)
writer.writeheader()
for entry in n.avail:
    # clean up for printing
    if 'state' in entry:
        # truncate state
        entry['state'] = entry['state'][0:5]
    if 'node' in entry:
        # pad node name
        entry['node'] = "{} ".format(entry['node'])
    # calculate percent mem
    entry['pcnt_mem'] = float(entry['allocmem']) / float(entry['totalmem']) * 100
    entry['pcnt_mem'] = "{0:.1f}".format(entry['pcnt_mem'])
    entry.pop('totalmem', None)

    writer.writerow(entry)
