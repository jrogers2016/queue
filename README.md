# queue
Minimal queue system for a single workstation

To enable multiple workstations, start a scheduler on the primary station and a different worker process on each secondary station

# usage

```
python scheduler.py --workers 4 # start scheduler with 4 workers and no gpu ids
python worker.py --workers 4 # start a guest server with 4 workers
python qsub.py test.sh # submit a shell script based task file
python qstat.py # list of jobs submitted to the scheduler

# The first line can alternatively be:
# python scheduler.py --workers 4 --gpuids 1,2,3,4 # start scheduler with 4 workers and the list of gpuids [1, 2, 3, 4]
```
