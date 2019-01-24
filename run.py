#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
partition with the most idle nodes
partition with the most mixed nodes
number of cpus used & requested on clinical queue 'intellispace'
recommended best queue to submit to
"""
import util.slurm as slurm

# get the partitions and jobs info from the system
p = slurm.Partitions()
sq = slurm.Squeue()

# starting value of intellispace cpu usage
intellispace_cpus = 0
# limit on allowed intellispace queue usage
intellispace_cpus_limit = 80

# default best queue
best_queue = "cpu_short"

# do not submit to these queues
partition_blacklist = [
"data_mover",
"cpu_dev",
"gpu4_dev",
"fn_short",
"fn_medium",
"fn_long",
"cpu_long"
]

most_idle = p.most_idle_nodes(blacklist = partition_blacklist)
most_mixed = p.most_mixed_nodes(blacklist = partition_blacklist)

# count the number of intellispace cpus used
for entry in sq.entries:
    if entry['PARTITION'] == 'intellispace':
        intellispace_cpus += int(entry['CPUS'])

# decide which queue is the best one to submit to based on current conditions
if intellispace_cpus < intellispace_cpus_limit:
    best_queue = 'intellispace'
elif most_idle:
    best_queue = most_idle
elif most_mixed:
    best_queue = most_mixed
else:
    pass

print("Partition with the most idle nodes: {0}".format(most_idle))
print("Partition with the most mixed nodes: {0}".format(most_mixed))
print("Intellispace CPUs used/requested: {0}".format(intellispace_cpus))
print("The best queue right now is: {0}".format(best_queue))

# print this to JSON later
d = {
'most_idle': most_idle,
'most_mixed': most_mixed,
'intellispace_cpus': intellispace_cpus,
'best_queue': best_queue
}
