#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Get SLURM availability summary

roughly analagous to:
sinfo -O 'allocmem,freemem,nodehost,partition'
"""
import util.slurm as slurm
import csv
import sys
n = slurm.Nodes()

writer = csv.DictWriter(sys.stdout, delimiter = '\t', fieldnames = n.avail[0].keys())
writer.writeheader()
for entry in n.avail:
    writer.writerow(entry)
