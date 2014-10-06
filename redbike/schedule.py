
import calendar
import csv
from datetime import datetime
import logging
import os
import random
import time

import redis
from dateutil.rrule import rrulestr


def _e(something):  # encode utf-8 if bytes. for py3 compat.
    if isinstance(something, bytes):
        return something.decode('utf-8')
    else:
        return something


SIGNALS = {"HALT": "HALT"}


class StopWork(Exception):
    """Just schedule the current job to STOP."""


class UnsetJob(Exception):
    """Unset the job from the queue."""


ENQUEUE_LUA = """
local workqueue = ARGV[1]
local jobid = ARGV[2]
local timestamp = tonumber(ARGV[3])
local is_working_key = workqueue .. "-" .. jobid
local members_key = workqueue .. "-members", jobid
if (redis.call("SISMEMBER", members_key, jobid) == 0
    and redis.call("GET", is_working_key) == false) then
   redis.call("LPUSH", workqueue, jobid)
   redis.call("SADD", workqueue .. "-members", jobid)
   redis.call("HSET", redbike.statuses_key, jobid, "ENQ:" .. timestamp)
   return timestamp
end"""

CONSUME_LUA = """
local workqueue = ARGV[1]
local timeout_seconds = ARGV[2]
local timestamp = tonumber(ARGV[3])
local jobtag = ARGV[4]
local jobid = redis.call("RPOP", workqueue)
if jobid ~= false then
   local is_working_key = workqueue .. "-" .. jobid
   local members_key = workqueue .. "-members", jobid
   redis.call("SREM", members_key, jobid)
   redis.call("SET", is_working_key, jobtag)
   redis.call("EXPIRE", is_working_key, timeout_seconds)
   redis.call("HSET", redbike.statuses_key, jobid, "WRK:" .. timestamp)
end
return jobid"""


class Redbike(object):

    def __init__(self, worker, prefix=None, redis_config=None, timefile=None,
                 log=None, stop_event=None, default_timeout=10):
        self.worker = worker
        self.prefix = prefix or 'redbike'
        self.redis = redis.StrictRedis(**(redis_config or {}))
        self.timefile = timefile or '.redbike.timefile'
        self.log = log if log else logging.getLogger('redbike-%s' % prefix)
        self.stop_event = stop_event
        self.default_timeout = default_timeout
        self.statuses_key = '%s-statuses' % self.prefix
        self.schedules_key = '%s-schedules' % self.prefix
        self.timeline_key = '%s-timeline' % self.prefix
        self.control_key = '%s-control' % self.prefix
        self.enqueue_script = self._register_script(ENQUEUE_LUA)
        self.consume_script = self._register_script(CONSUME_LUA)
        # Capture queue_name generator here so that after halting
        # subsequent calls to work() don't just keep hitting the first queue.
        self.consumer = self.consumer_generator()

    def _register_script(self, lua):
        redbike_env = {"statuses_key": self.statuses_key}
        redbike_env_lua = "local redbike = {%s}" % ",".join(
            "=".join([k, repr(v)]) for k, v in redbike_env.items())
        return self.redis.register_script(redbike_env_lua + lua)

    def control(self, signal):
        self.redis.set(self.control_key, SIGNALS[signal.upper()])

    def is_halted(self):
        if self.stop_event is not None and self.stop_event.is_set():
            return True  # pragma: no cover
        return _e(self.redis.get(self.control_key)) == "HALT"

    def clear_control(self):
        self.redis.delete(self.control_key)

    def set_status(self, jobid, event, timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        self.redis.hset(self.statuses_key,
                        jobid,
                        "%s:%s" % (event, int(timestamp)))

    def set_schedule(self, jobid, schedule):
        self.redis.hset(self.schedules_key, jobid, schedule)

    def set(self, jobid, schedule, after=None):
        self.set_schedule(jobid, schedule)
        self.schedule(jobid, schedule, after=after)

    def unset(self, jobid):
        self.redis.hdel(self.statuses_key, jobid)
        self.redis.hdel(self.schedules_key, jobid)
        self.redis.zrem(self.timeline_key, jobid)
        self.remove_from_queue(jobid)

    def add_to_timeline(self, jobid, timestamp):
        self.set_status(jobid, 'TML')
        self.redis.zadd(self.timeline_key, int(timestamp), jobid)

    def queue_for(self, jobid):
        return "%s-%s" % (self.prefix, self.worker.queue_for(jobid))

    def enqueue(self, jobid):
        self.enqueue_script(
            args=[self.queue_for(jobid), jobid, int(time.time())])

    def schedule(self, jobid, schedule, after=None, backoff=None):
        jobid = _e(jobid)
        schedule = _e(schedule)
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

    def reschedule(self, jobid, jobtag, backoff=None):
        if self.recycle(jobid, jobtag):
            schedule = self.redis.hget(self.schedules_key, jobid)
            self.schedule(jobid, schedule, backoff=backoff)

    def load_csv(self, csvfilename):
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

    def remove_from_queue(self, jobid):
        return self.redis.lrem(self.queue_for(jobid), 0, jobid)

    def queue_names(self):
        return ["%s-%s" % (self.prefix, queue_name)
                for queue_name in self.worker.queue_names()]

    def consumer_generator(self):
        while True:
            for queue_name in self.queue_names():
                jobtag = '%030x' % random.randrange(16**30)
                timeout = self.worker.timeout(queue_name)
                if timeout is None:
                    timeout = self.default_timeout
                args = [queue_name, timeout, int(time.time()), jobtag]
                yield (_e(self.consume_script(args=args)), jobtag)

    def is_working_key(self, jobid):
        return "%s-%s" % (self.queue_for(jobid), _e(jobid))

    def recycle(self, jobid, jobtag):
        working_key = self.is_working_key(jobid)
        if _e(self.redis.get(working_key)) == _e(jobtag):
            return self.redis.delete(working_key)
        else:
            return False

    def is_working(self, jobid):
        return self.redis.exists(self.is_working_key(jobid))

    def work(self):
        for jobid, jobtag in self.consumer:
            if jobid:
                try:
                    backoff = None
                    try:
                        backoff = self.worker.work(jobid)
                    except StopWork:
                        self.set_schedule(jobid, 'STOP')
                    except UnsetJob:
                        self.unset(jobid)
                        self.recycle(jobid, jobtag)
                        continue
                    self.reschedule(jobid, jobtag, backoff=backoff)
                except Exception as ex:
                    self.log.exception(ex)
                    self.set_status(jobid, 'DIE')
                    self.recycle(jobid, jobtag)
            if self.is_halted():
                self.log.info("stopping on command")
                break

    def get_statuses(self, before=None):
        if before is None:
            before = time.time()
        before = int(before)
        all_statuses = self.redis.hgetall(self.statuses_key)
        for jobid, status in all_statuses.items():
            event, timestamp = _e(status).split(':')
            timestamp = int(timestamp)
            if timestamp <= before:
                yield _e(jobid), event, timestamp

    def get_schedules(self):
        return self.redis.hgetall(self.schedules_key).items()

    def flush(self):
        keys = self.redis.keys("%s-*" % self.prefix)
        if keys:
            return self.redis.delete(*keys)
        return 0

    def tell(self, jobid):
        return {"status": _e(self.redis.hget(self.statuses_key, jobid)),
                "schedule": _e(self.redis.hget(self.schedules_key, jobid)),
                "next_run": self.redis.zscore(self.timeline_key, jobid),
                "working": self.is_working(jobid)}


class RoundRobin(object):

    def __init__(self, initstring):
        self.initstring = _e(initstring)

    def name_queue(self, code):
        return 'work-%s' % _e(code)

    def queue_for(self, jobid):
        return self.name_queue(_e(jobid).split(':')[-1])

    def queue_names(self):
        return [self.name_queue(x) for x in self.initstring.split(':')]

    def work(self, jobid):
        raise NotImplemented  # pragma: no cover

    def timeout(self, queue_name):
        return None
