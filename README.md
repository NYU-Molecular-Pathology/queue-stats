# queue-stats

Programs to monitor and display HPC cluster resource usage and availability.

## Stand-alone SLURM scripts

These scripts have been designed to be portable and useable in a stand-alone manner, depending only on basic Python libraries. They can be copied directly from this repository and run on your system (no installation needed).

### `savail`

Script to display per-node resource availability, including node's free memory, allocated memory, percentage allocated memory, number of CPUs (allocated/total), state, and partitions.


```
$ savail
node      allocmem   freemem  pcnt_mem    cpus  state  partitions
cn-0003      0.0GB   370.3GB       0.0   40/40  compl  cpu_dev
cn-0004    314.0GB   327.1GB      98.1    4/40  mixed  cpu_short,cpu_medium
cn-0005    294.0GB   339.4GB      91.9    9/40  mixed  cpu_short,cpu_medium
cn-0006    282.0GB   282.3GB      88.1   21/40  mixed  cpu_short,cpu_medium
```


### `stotal`

Calculates the total resource usage for the current user (default).

```
$ stotal
Running: 106 CPUs, 1012.0GB memory, 40 jobs
Pending: 83 CPUs, 898.0GB memory, 41 jobs
```

A username may be passed to see someone else's usage:

```
$ stotal -u some_username
Running: 400 CPUs, 1600.0GB memory, 400 jobs
Pending: 2196 CPUs, 8784.0GB memory, 2196 jobs
```

### `suser`

Outputs total usage metrics per-user, per-partition. 

```
$ suser
cpu_long (jobs)
---------
user1  : 6
user2  : 1
user3  : 1
user4  : 1


cpu_medium (jobs)
---------
user5  : 329
user6  : 64
user7  : 39

```

Can also display other metrics with `mem`, `cpus`, and `time` (`jobs` is the default).

```
$ suser mem
cpu_long (mem)
---------
user1    : 192.0G
user2    : 60.0G
user3    : 16.0G
user4    : 4.0G


cpu_medium (mem)
---------
user5   : 1308.0G
user6   : 512.0G
user7   : 312.0G
```

```
$ ./suser time
cpu_long (time)
---------
user1    : 687.2hr
user2    : 213.2hr
user3    : 25.6hr
user4    : 2.0hr


cpu_medium (time)
---------
user5   : 3572.9hr
user6   : 1543.7hr
user7   : 362.5hr
```

```
$ ./suser cpus
cpu_long (cpus)
---------
user1    : 120
user2    : 1
user3    : 1
user4    : 1


cpu_medium (cpus)
---------
user5   : 327
user6   : 156
user7   : 64
user8   : 13
```


### `req`

Shell script to requeue all pending jobs to a given queue. Command usage is: `req <queue_to> [<queue_from>]`

```
# requeue all jobs to cpu_short
req cpu_short

# requeue jobs in gpu4_short to gpu4_long
req gpu4_short gpu4_long
```

----

# Non-standalone Programs

## qstats.py

Program to save parsed information from `sinfo` and `squeue` output from SLURM HPC cluster to a file for parsing by other programs.

Use this with `cron` to check the cluster at set intervals, to reduce load on the HPC cluster by programs which need to query this information frequently. Also mitigates errors from `sinfo` and `squeue` affecting your other programs by 'cache'ing the last known state of the cluster in a file.

The current implementation simply evaluates the state of the cluster partitions for the queues with the most idle and 'mixed' nodes, and counts the number of CPUs used/requested by 'intellispace' queue, in order to make a decision on the best queue to submit to. Other programs can read this output and dynamically adjust cluster submission configurations to best take advantage of the available resources.

# Usage

Clone this repo:

```
git clone --recursive https://github.com/NYU-Molecular-Pathology/queue-stats.git
cd queue-stats
```

Run the program:

- terminal output

```
./qstats.py
[2019-01-24T16:23:41] best queue: intellispace, most idle: gpu4_short, most mixed: cpu_short, intellispace cpus: 0
```

- JSON output

```
$ cat slurm.json
{
    "best_queue": "intellispace",
    "updated": "2019-01-24T16:25:01",
    "most_mixed": "cpu_short",
    "intellispace_cpus": 0,
    "most_idle": "gpu4_short"
}
```

Create a `cron` entry for automated execution:

```
# run every 5 minutes
$ make cron
*/5 * * * * . /gpfs/home/kellys04/.bash_profile; cd /gpfs/home/kellys04/queue-stats; python qstats.py >> qstats.log 2>&1
```

Add the new entry to you crontab with `crontab -e`.


# Software

- Python (2.7+)

- `cron`
