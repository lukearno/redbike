
import calendar
import csv
from datetime import datetime
import logging
import os
import time

import redis
from dateutil.rrule import rrulestr


SIGNALS = {"HALT": "HALT"}


class StopWork(Exception):
    """Just schedule the current job to STOP."""


class Redbike(object):

    def __init__(self, worker, prefix=None, redis_config=None, timefile=None,
                 log=None, stop_event=None):
        self.worker = worker
        self.prefix = prefix or 'redbike'
        self.redis = redis.StrictRedis(**(redis_config or {}))
        self.timefile = timefile or '.redbike.timefile'
        self.log = log if log else logging.getLogger('redbike-%s' % prefix)
        self.stop_event = stop_event
        self.statuses_key = '%s-statuses' % self.prefix
        self.schedules_key = '%s-schedules' % self.prefix
        self.timeline_key = '%s-timeline' % self.prefix
        self.control_key = '%s-control' % self.prefix
        # Capture queue_name generator here so that after halting
        # subsequent calls to work() don't just keep hitting the first queue.
        self.consumer = self.worker.consume(self)

    def control(self, signal):
        self.redis.set(self.control_key, SIGNALS[signal.upper()])

    def is_halted(self):
        if self.stop_event is not None and self.stop_event.is_set():
            return True  # pragma: no cover
        return self.redis.get(self.control_key) == "HALT"

    def clear_control(self):
        self.redis.delete(self.control_key)

    def set_status(self, jobid, event, timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        self.redis.hset(self.statuses_key,
                        jobid,
                        "%s:%s" % (event, int(timestamp)))

    def set_schedule(self, jobid, schedule):
        self.redis.hset('%s-schedules' % self.prefix, jobid, schedule)

    def set(self, jobid, schedule, after=None):
        self.set_schedule(jobid, schedule)
        self.schedule(jobid, schedule, after=after)

    def unset(self, jobid):
        self.redis.hdel(self.statuses_key, jobid)
        self.redis.hdel(self.schedules_key, jobid)
        self.redis.zrem(self.timeline_key, jobid)

    def add_to_timeline(self, jobid, timestamp):
        self.set_status(jobid, 'TML')
        self.redis.zadd(self.timeline_key, int(timestamp), jobid)

    def enqueue(self, jobid):
        self.set_status(jobid, 'ENQ')
        self.worker.enqueue(self, jobid)

    def schedule(self, jobid, schedule, after=None, backoff=None):
        if schedule is None:
            self.unset(jobid)
        elif schedule == 'STOP':
            self.set_status(jobid, 'STP')
        elif schedule == 'CONTINUE' and backoff:
            self.add_to_timeline(jobid, int(time.time()) + backoff)
        elif schedule == 'CONTINUE':
            self.enqueue(jobid)
        elif schedule == 'NOW':
            self.set_schedule(jobid, 'STOP')
            self.enqueue(jobid)
        elif schedule.startswith("AT:"):
            self.set_schedule(jobid, 'STOP')
            self.add_to_timeline(jobid, schedule.split(":")[1])
        else:
            after_dt = (datetime.fromtimestamp(after)
                        if after else datetime.utcnow())
            try:
                rrule = rrulestr(schedule)
                next_run_dt = rrule.after(after_dt)
                if next_run_dt:
                    next_run = calendar.timegm(next_run_dt.timetuple())
                    self.add_to_timeline(jobid, next_run)
                else:
                    self.set_status(jobid, 'STP')
            except ValueError:
                self.set_status(jobid, 'BAD')
                self.log.warn("%s Bad RRULE", jobid)

    def reschedule(self, jobid, backoff=None):
        schedule = self.redis.hget(self.schedules_key, jobid)
        self.schedule(jobid, schedule, backoff=backoff)

    def load_csv(self, csvfilename):
        # TODO: This should be pipelined.
        with open(csvfilename) as csvfile:
            reader = csv.reader(csvfile)
            for jobid, schedule in reader:
                self.set(jobid, schedule)

    def point_in_time(self):
        if os.path.exists(self.timefile):
            with open(self.timefile) as timefile:
                return int(timefile.read())
        else:
            return time.time()

    def dispatch(self, after=None, csvfilename=None):
        if csvfilename:
            self.load_csv(csvfilename)
        if after is not None:
            point_in_time = after
        else:
            point_in_time = self.point_in_time()
        point_in_time = int(point_in_time)
        while True:
            outstanding = self.redis.zrangebyscore(
                self.timeline_key, 0, point_in_time)
            for jobid in outstanding:
                self.redis.zrem(self.timeline_key, jobid)
                self.enqueue(jobid)
            time.sleep(.01)
            point_in_time = int(time.time())
            with open('%s.0' % self.timefile, 'w') as timefile:
                timefile.write(str(point_in_time))
            os.rename('%s.0' % self.timefile, self.timefile)
            if self.is_halted():
                self.log.info("stopping on command")
                break

    def work(self):
        for jobid in self.consumer:
            if jobid:
                self.set_status(jobid, 'WRK')
                try:
                    try:
                        backoff = self.worker.work(self, jobid)
                    except StopWork:
                        self.set_schedule(jobid, 'STOP')
                    self.reschedule(jobid, backoff=backoff)
                except Exception as ex:
                    self.log.exception(ex)
                    self.set_status(jobid, 'DIE')
            if self.is_halted():
                self.log.info("stopping on command")
                break

    def get_statuses(self, before=None):
        if before is None:
            before = time.time()
        before = int(before)
        all_statuses = self.redis.hgetall(self.statuses_key)
        for jobid, status in all_statuses.iteritems():
            event, timestamp = status.split(':')
            timestamp = int(timestamp)
            if timestamp <= before:
                yield jobid, event, timestamp

    def get_schedules(self):
        return self.redis.hgetall(self.schedules_key).iteritems()

    def flush(self):
        self.redis.delete(*(self.worker.queue_names(self)
                            + [self.statuses_key,
                               self.schedules_key,
                               self.timeline_key,
                               self.control_key]))

    def tell(self, jobid):
        return {"status": self.redis.hget(self.statuses_key, jobid),
                "schedule": self.redis.hget(self.schedules_key, jobid),
                "next_run": self.redis.zscore(self.timeline_key, jobid)}


class RoundRobin(object):

    def __init__(self, initstring):
        self.initstring = initstring

    def name_queue(self, bike, code):
        return '%s-work-%s' % (bike.prefix, code)

    def queue_for(self, bike, jobid):
        return self.name_queue(bike, jobid.split(':')[-1])

    def enqueue(self, bike, jobid):
        return bike.redis.lpush(self.queue_for(bike, jobid), jobid)

    def queue_names(self, bike):
        return [self.name_queue(bike, x)
                for x in self.initstring.split(':')]

    def consume(self, bike):
        while True:
            for queue_name in self.queue_names(bike):
                yield bike.redis.rpop(queue_name)

    def work(self, bike, jobid):
        raise NotImplemented  # pragma: no cover
