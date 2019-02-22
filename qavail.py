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

writer = csv.DictWriter(sys.stdout, delimiter = '\t', fieldnames = n.avail[0].keys())
writer.writeheader()
for entry in n.avail:
    # clean up for printing
    if 'state' in entry:
        entry['state'] = entry['state'][0:5]
    if 'node' in entry:
        entry['node'] = "{} ".format(entry['node'])
    writer.writerow(entry)
