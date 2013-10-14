# Redbike Sched

A simple scheduler using Redis and iCal rrules.

Redbike is designed to queue recurring jobs to a pool of
distributed workers, with the following requirements:

* Only one worker works a given job at a time.
* There are no dependencies between jobs.
* The JOBID string contains everything required to distribute and work the job.

Redbike is available from the Python Package Index:

```bash
$ pip install redbike
```
## High Level

Redbike has a simple design

* A registry of job schedules (a [hash](http://redis.io/topics/data-types#hashes))
* A registry of job statuses (another [hash](http://redis.io/topics/data-types#hashes)) format: "EVENT:TIMESTAMP"
* A timeline of scheduled work (a [sorted set](http://redis.io/topics/data-types#sorted-sets) scored by timestamp of next run)
* Any number of job queues ([lists](http://redis.io/topics/data-types#lists))
* A single dispatcher process to enqueue work that is due on the timeline
* Any number of worker processes

## Configuration

```bash
$ redbike --config=/etc/redbike.conf ...
```

Redbike needs to know the worker class, the prefix to use for its
keys in Redis and the location of the time file (see below). Also
it needs to know how to connect to Redis. By default it looks in 
`.redbike.conf` but you can specify any file.

Connection information is found in the `[redbike-redis]` stanza and
mirrors the arguments of py-redis's `StrictRedis` class. The rest
of the settings are found in the `[redbike]` stanza.
Other information in the config file is ignored, so you can use
a shared config as long as it has something like this in it:

```ini
[redbike]
prefix: myapp
worker: somepackage.somemodule:MyWorkerClass('initstring')
timefile: /var/log/myapp.redbike.timefile
```

## Entering Jobs

```bash
$ redbike set <JOBID> <SCHEDULE> [--after=<TIMESTAMP>]
```
To run jobs in redbike just enter a JOBID and schedule. Schedules
can be in iCal [RRULE](http://www.kanzaki.com/docs/ical/rrule.html)
format or one of the following special values:

* `AT:TIMESTAMP` - run once at the specified time
* `CONTINUE` - just re-queue right away
* `STOP` - don't run anymore
 
If `--after` is provided, it will override the timefile (see below)
and when (or if) to schedule each job will be based on this value.

The set operation does not wait for the dispatcher, and enters
the job directly into the timeline or work queue where
appropriate.

## Dispatching Work

```bash
$ redbike dispatch [<WORKER>] [--schedules=<SCHEDULESCSV> [--after=<TIMESTAMP>]]
```

Dispatch keeps checking the timeline and places jobs into work queues
when they are due. It can also take a CSV of JOBID, SCHEDULE 
pairs and an optional after timestamp that works the same way 
as `redbike set` at startup time. 

The dispatcher continually updates the contents of the timefile with
the current timestamp as it runs. When starting dispatch or setting
schedules, the time in the timefile is used to determine when the
the next run of the job should occur, if any. When no `--after`
argument is provided and no timefile is found, the default value is
the current time.

A CSV of JOBID,SHEDULE pairs can also be provided. These schedules
will be set before dispatch begins. 

## Consuming Work

```bash
$ redbike work [<WORKER>]
```

Redbike consumes jobs from the work queues using the worker class's
`consume()` method and passes them to the worker's `work()` method.
If `StopWork` is raised, the job's schedule is set to `STOP`. If
any other exception is raised it is logged and the job is not 
rescheduled. Otherwise the job's schedule is checked and it's 
returned to the queue or timeline if appropriate.

## Writing a Worker

Redbike includes a simple worker class that is flexible enough for
many cases. Just override its `work()` method. 

```python
# mymodule.py
from redbike import RoundRobbin, StopSchedule

class Worker(RoundRobbin):

    def work(self, bike, jobid):
        should_stop = do_something(jobid)
        if should_stop:
            raise StopWork("Don't run this job any more!")
```

This class chooses work queues based on the portion of the
JOBID following the last `:`. It is initialized with a colon
separated string specifying the queues to work and the order
in which to work them. If, for instance, you want to put jobs
into two queues and have queue "A" worked twice as often as
queue "B", this would do the trick:

```bash
$ redbike set job1:A CONTINUE
$ redbike set job2:B CONTINUE
$ redbike work mymodule:Work("A:A:B")
```
## Stopping

```bash
$ redbike control HALT
```

Both dispatcher and worker processes watch for a control key
to be set to `HALT` in Redis and stop immediately after completing
their current task.

## Failure Modes

Redbike does its best to fail gracefully but managing the failure 
modes of your workload is out of scope. How to approach it 
depends on how you feel about things like the same scheduled 
instance of a job getting worked twice or getting missed when
things crash and burn. That said, there are a few commands that
can help.

Jobs that may have fallen out of circulation and need to be reset
can be found by time of their last status.

```bash
$ redbike statuses [--before=<TIMESTAMP>]
JOBID,status
JOBID,status
...
```

Output is in CSV format.

Statuses are set throughout the lifecycle of jobs in Redbike.

* TML - entered into the timeline
* ENQ - entered into a work queue
* BAD - failed to schedule due to a bad RRULE
* WRK - picked up by a worker
* STP - stopped when the worked raise StopWork
* DIE - worker raised an unexpected exception

To dump the schedules of all the jobs:

```bash
$ redbike schedules
JOBID,schedule
JOBID,schedule
...
```

Again, output is in CSV format.

Finally, to spy on an individual job:

```bash
$ redbike tell <JOBID>
```

This will output a json representation like this:

```json
{
  "status": "ENQ:1381720578", 
  "next_run": null, 
  "schedule": "CONTINUE"
}
```