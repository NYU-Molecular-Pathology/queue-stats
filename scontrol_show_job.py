#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
$ scontrol show job
"""
import subprocess as sp
command = ['scontrol', 'show', 'job']
process = sp.Popen(command,
                    stdout = sp.PIPE,
                    stderr = sp.PIPE,
                    shell = False,
                    universal_newlines = True)
# run the command, capture stdout and stderr
proc_stdout, proc_stderr = process.communicate()
proc_stdout_lines = proc_stdout.split('\n')
# print(proc_stdout_lines)
jobIndex = 0
jobs = {}
for line in proc_stdout_lines:
    # check for empty line; try to parse
    if not line == '':
        # initialize dict entry
        if jobIndex not in jobs:
            jobs[jobIndex] = {}
        # break
        # 'Command' entries have multiple spaces... skip them for now
        if not line.strip().startswith('Command='):
            # split line on whitespace
            parts = line.split()
            for part in parts:
                # print(part)
                if '=' in part:
                    # split on first =
                    key, values = part.split('=', 1)
                    # check for 'TRES' which has further values
                    if key == 'TRES':
                        groupings = values.split(',')
                        for grouping in groupings:
                            key2, value2 = grouping.split('=')
                            pass # parse this correctly...
                    else:
                        jobs[jobIndex][key] = values
                else:
                    # if there is no '=' in the remainder, skip it
                    pass
    else:
        jobIndex += 1
        # break
for key, values in jobs.items():
    print(key, values)
    print('')
    print('')
    pass
