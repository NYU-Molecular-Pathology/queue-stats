# queue-stats

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
