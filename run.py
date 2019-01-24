#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
partition with the most idle nodes
partition with the most mixed nodes
number of cpus used & requested on clinical queue 'intellispace'
recommended best queue to submit to
"""
import util.slurm as slurm

p = Partitions()
partition_blacklist = [
"data_mover",
"cpu_dev",
"gpu4_dev",
"fn_short",
"fn_medium",
"fn_long",
"cpu_long"
]
print("Partition with the most idle nodes: {0}".format(p.most_idle_nodes(blacklist = partition_blacklist)))
print("Partition with the most mixed nodes: {0}".format(p.most_mixed_nodes(blacklist = partition_blacklist)))
intellispace_cpus = 0
sq = Squeue()
for entry in sq.entries:
    if entry['PARTITION'] == 'intellispace':
        intellispace_cpus += int(entry['CPUS'])
print("Intellispace CPUs used/requested: {0}".format(intellispace_cpus))
