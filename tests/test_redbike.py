from datetime import datetime, timedelta
import os
import time

from unittest import TestCase

from flexmock import flexmock

from redbike import Redbike, RoundRobin, StopWork, UnsetJob
from redbike.schedule import _e  # for py3 compat


class TestWorker(RoundRobin):

    def work(self, jobid):
        self.bike.redis.hincrby('biketest-results', jobid, amount=1)
        assert self.bike.is_working(jobid), "redbike knows job is working"
        if jobid.startswith('backoff:'):
            return int(jobid[8:9])
        if jobid.startswith('stopper:'):
            raise StopWork("Stop this job.")
        if jobid.startswith('fail:'):
            raise Exception("Boom")
        if jobid.startswith('unset:'):
            raise UnsetJob("Unset this job")
        if jobid.endswith('Z'):
            time.sleep(2)

    def timeout(self, queue_name, default=None):
        if queue_name.endswith('Z'):
            return 1
        else:
            return default


class RedbikeTests(TestCase):

    def setUp(self):
        self.bike = Redbike(TestWorker('A:Z'), prefix='biketest')
        self.bike.worker.bike = self.bike
        self.r = self.bike.redis  # for convenience
        self.bike.flush()
        self.r.delete('biketest-results')
        self.bike.control("HALT")

    def queue(self, name='A'):
        return list(map(_e, self.r.lrange('biketest-work-%s' % name, 0, -1)))

    def result(self, jobid):
        return _e(self.r.hget('biketest-results', jobid))

    def schedules(self):
        return {k: _e(v) for k, v
                in self.r.hgetall(self.bike.schedules_key).items()}

    def timeline(self):
        return list(map(_e, self.r.zrange(self.bike.timeline_key, 0, -1)))

    def report(self, queue_names=None):
        if queue_names is None:
            queue_names = ['A']
        print("SCHEDULES", self.schedules())
        print("TIMELINE", self.timeline())
        for qn in queue_names:
            print("QUEUE %s:" % qn, self.queue(name=qn))

    def gen_rrule(self):
        return "DTSTART:%s\nRRULE:FREQ=SECONDLY" % (
            (datetime.utcnow() - timedelta(minutes=1)).isoformat())

    def work_round(self):
        self.bike.work()
        self.bike.work()

    def test_set_and_unset(self):
        #B: Setting a job populates the attendant data structures in Redis.
        self.assertEqual(self.bike.tell('job:A'),
                         {'status': None,
                          'next_run': None,
                          'schedule': None,
                          'working': False})
        self.bike.set('job:A', 'AT:%s' % int(time.time() + 2))
        tell = self.bike.tell('job:A')
        self.assertTrue(tell['status'].startswith('TML:'))
        self.assertNotEqual(tell['next_run'], None)
        #B: Un-setting a job clears the attendant dgg1ata structures in Redis.
        self.bike.unset('job:A')
        self.assertEqual(self.bike.tell('job:A'),
                         {'status': None,
                          'next_run': None,
                          'schedule': None,
                          'working': False})
        #B: Un-setting a job does remove it from the work queue.
        self.bike.set('job:A', 'CONTINUE')
        self.assertEqual(self.queue(), ['job:A'])
        self.bike.unset('job:A')
        self.assertEqual(self.queue(), [])
        #B: An unset job is removed from queue.
        self.work_round()
        self.assertEqual(self.bike.tell('job:A'),
                         {'status': None,
                          'next_run': None,
                          'schedule': None,
                          'working': False})
        self.assertEqual(self.queue(), [])
        #B: Setting a None schedule is just like unsetting.
        self.bike.set('job:A', None)
        self.assertEqual(self.bike.tell('job:A'),
                         {'status': None,
                          'next_run': None,
                          'schedule': None,
                          'working': False})
        self.assertEqual(self.queue(), [])

    def test_continue_and_stop(self):
        self.bike.set('job:A', 'CONTINUE')
        tell = self.bike.tell('job:A')
        #B: Setting a job to CONTINUE registers CONTINUE in the schedules.
        self.assertEqual(tell['schedule'], 'CONTINUE')
        #B: Setting a job to CONTINUE registers ENQ status.
        self.assertTrue(tell['status'].startswith('ENQ:'))
        #B: Setting a job to CONTINUE enters it in the work queue.
        self.assertEqual(self.queue(), ['job:A'])
        #B: Setting a job to CONTINUE skips the timeline.
        self.assertEqual(tell['next_run'], None)
        #B: A CONTINUE job goes right back in the work queue.
        self.work_round()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(['job:A'], self.queue())
        #B: Successfully worked job shows as no longer working.
        self.assertFalse(self.bike.is_working('job:A'))
        #B: A CONTINUE job set to STOP is not requeued.
        self.bike.set('job:A', 'STOP')
        self.work_round()
        self.assertEqual(self.result('job:A'), '2')
        self.assertEqual(self.queue(), [])

    def test_continue_with_backoff(self):
        self.bike.set('backoff:2:A', 'CONTINUE')
        #B: Setting a job to CONTINUE skips the timeline.
        self.assertEqual(self.timeline(), [])
        self.assertEqual(self.queue(), ['backoff:2:A'])
        #B: A CONTINUE job with backoff goes into the timeline.
        self.work_round()
        self.assertEqual(self.result('backoff:2:A'), '1')
        self.assertEqual(self.timeline(), ['backoff:2:A'])
        self.assertEqual(self.queue(), [])
        #B: A CONTINUE job with backoff runs again after specified backoff.
        self.bike.dispatch(after=time.time() + 2)
        self.assertEqual(self.timeline(), [])
        self.assertEqual(self.queue(), ['backoff:2:A'])

    def test_rrule(self):
        self.bike.set('job:A', self.gen_rrule())
        tell = self.bike.tell('job:A')
        #B: Setting a job to RRULE puts a TML event in statuses.
        self.assertTrue(tell['status'].startswith('TML:'))
        #B: Setting a job to RRULE enters the job in the timeline.
        self.assertEqual(self.timeline(), ['job:A'])
        #B: Setting a job to RRULE does not go straight to the work queue.
        self.assertEqual(self.queue(), [])
        #B: An RRULE is not put in the work queue by the schedule when not due.
        self.bike.dispatch()
        self.assertEqual(self.queue(), [])
        #B: An RRULE is queued up by the scheduler once it is due.
        self.bike.dispatch(after=time.time() + 2)
        self.assertEqual(self.queue(), ['job:A'])
        #B: Setting a job to RRULE goes back in the timeline after it is run.
        self.work_round()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(self.queue(), [])
        self.assertEqual(self.timeline(), ['job:A'])

    def test_bad_rrule(self):
        self.bike.set('job:A', "DTSTART:20131009T164510\nR:FREQ=Secondly")
        tell = self.bike.tell('job:A')
        #B: Setting a job to a bad RRULE puts a BAD event in statuses.
        self.assertTrue(tell['status'].startswith('BAD:'))
        #B: Setting a job to a bad RRULE does not queue or schedule.
        self.assertEqual(self.timeline(), [])
        self.assertEqual(self.queue(), [])

    def test_rrule_runs_out(self):
        self.bike.set('job:A', self.gen_rrule()+";COUNT=1")
        tell = self.bike.tell('job:A')
        #B: Scheduling an rrule that has run out registers a STP event.
        self.assertTrue(tell['status'].startswith('STP:'))

    def test_now(self):
        self.bike.set('job:A', 'NOW')
        tell = self.bike.tell('job:A')
        #B: Setting a job to NOW registers STOP in the schedules.
        self.assertEqual(tell['schedule'], 'STOP')
        #B: Setting a job to NOW registers ENQ status.
        self.assertTrue(tell['status'].startswith('ENQ:'))
        #B: Setting a job to CONTINUE enters it in the work queue.
        self.assertEqual(self.queue(), ['job:A'])
        #B: Setting a job to CONTINUE skips the timeline.
        self.assertEqual(tell['next_run'], None)
        #B: A NOW job does not run again.
        self.work_round()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(self.queue(), [])
        #B: A stop event is registered after the NOW job is run.
        tell = self.bike.tell('job:A')
        self.assertTrue(tell['status'].startswith('STP:'))

    def test_at(self):
        self.bike.set('job:A', 'AT:%s' % int(time.time() + 2))
        tell = self.bike.tell('job:A')
        #B: Setting a job to AT:TIMESTAMP puts a TML event in statuses.
        self.assertTrue(tell['status'].startswith('TML:'))
        #B: Setting a job to AT:TIMESTAMP enters it into the timeline.
        self.assertEqual(self.timeline(), ['job:A'])
        #B: Setting a job to AT:TIMESTAMP does not go straight to thequeue.
        self.assertEqual(self.queue(), [])
        #B: An AT:TIMESTAMP is not queued by the schedule when not due.
        self.bike.dispatch()
        self.assertEqual(self.queue(), [])
        #B: An AT:TIMESTAMP is queued up by the scheduler once it is due.
        self.bike.dispatch(after=time.time() + 3)
        self.assertEqual(self.queue(), ['job:A'])
        #B: An AT:TIMESTAMP job does not go back into the timeline or queue.
        self.work_round()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(self.timeline(), [])
        self.assertEqual(self.queue(), [])
        #B: A stop event is registered after the AT:TIME job is run.
        tell = self.bike.tell('job:A')
        self.assertTrue(tell['status'].startswith('STP:'))

    def test_dispatch_with_csv(self):
        #B: Dispatching with a CSV of job,schedule records schedules the jobs.
        self.bike.dispatch(csvfilename='tests/test.csv')
        sched = {_e(k): _e(v) for k, v in self.bike.get_schedules()}
        self.assertEqual(sched['job1:A'], 'CONTINUE')
        self.assertEqual(sched['job2:B'], 'STOP')
        statuses = list(self.bike.get_statuses())
        self.assertEqual(len(statuses), 2)

    def test_dispatch_with_after(self):
        #B: Dispatching with an after overrides the timefile.
        flexmock(self.bike).should_receive('point_in_time').never
        self.bike.dispatch(after=8)

    def test_clear_control_dont_halt(self):
        #B: The check for halted returns false when control is cleared.
        self.bike.clear_control()
        self.assertFalse(self.bike.is_halted())

    def test_point_in_time_defaults_to_now(self):
        #B: Missing timefile means we fall back to now.
        os.rename(self.bike.timefile, 'tests/timefile.tmp')
        flexmock(time).should_receive('time').once.replace_with(lambda: 9)
        self.assertEqual(self.bike.point_in_time(), 9)
        os.rename('tests/timefile.tmp', self.bike.timefile)

    def test_stop_work(self):
        self.bike.set('stopper:A', 'CONTINUE')
        #B: Raising StopWork cause the job to be scheduled STOP.
        self.assertEqual(self.bike.tell('stopper:A')['schedule'], 'CONTINUE')
        self.work_round()
        self.assertEqual(self.result('stopper:A'), '1')
        self.assertEqual(self.bike.tell('stopper:A')['schedule'], 'STOP')
        #B: Raising StopWork cause the job to not run again.
        self.work_round()
        self.assertEqual(self.result('stopper:A'), '1')

    def test_unset_job_via_work_exception(self):
        self.assertEqual(self.queue(), [])
        self.assertEqual(self.result('unset:A'), None)
        self.assertEqual(self.schedules(), {})
        self.assertEqual(self.timeline(), [])

        #B: Raising UnsetJob causes the job to be unset
        self.bike.set(jobid='unset:A', schedule='CONTINUE')

        self.assertEqual(self.queue(), ['unset:A'])
        self.assertEqual(self.result('unset:A'), None)
        self.assertEqual(self.schedules(), {'unset:A': 'CONTINUE'})
        self.assertEqual(self.timeline(), [])
        tell = self.bike.tell('unset:A')
        self.assertEqual(tell['next_run'], None)
        self.assertEqual(tell['schedule'], 'CONTINUE')
        self.assertTrue(tell['status'].startswith('ENQ:'))
        self.assertEqual(tell['working'], False)

        self.work_round()

        self.assertEqual(self.queue(), [])
        self.assertEqual(self.result('unset:A'), '1')
        self.assertEqual(self.schedules(), {})
        self.assertEqual(self.timeline(), [])
        tell = self.bike.tell('unset:A')
        self.assertEqual(tell['next_run'], None)
        self.assertEqual(tell['schedule'], None)
        self.assertEqual(tell['status'], None)
        self.assertEqual(tell['working'], False)

    def test_job_blows_up(self):
        self.bike.set('fail:A', 'CONTINUE')
        self.assertEqual(self.queue(), ['fail:A'])
        #B: Raising an unexpected error is handled.
        self.assertEqual(self.bike.tell('fail:A')['schedule'], 'CONTINUE')
        self.work_round()
        #B: Job does not show up as working after it blows up.
        self.assertFalse(self.bike.is_working('fail:A'))
        self.assertEqual(self.result('fail:A'), '1')
        tell = self.bike.tell('fail:A')
        self.assertTrue(tell['status'].startswith('DIE:'))
        self.assertEqual(self.bike.tell('fail:A')['schedule'], 'CONTINUE')
        #B: When an unexpected error is raise the job is not rescheduled.
        self.work_round()
        self.assertEqual(self.result('fail:A'), '1')

    def test_outstanding_job_tracking(self):
        self.bike.set('job:A', 'NOW')
        #B: Job is removed from queue and is_working keys is set when consumed.
        self.assertFalse(self.bike.is_working('job:A'))
        self.assertEqual(self.queue(), ['job:A'])
        next(self.bike.consumer)
        self.assertTrue(self.bike.is_working('job:A'))
        self.assertEqual(self.queue(), [])
        #B: Enqueue is no-op if job is currently being worked.
        self.bike.set('job:A', 'NOW')
        self.assertTrue(self.bike.is_working('job:A'))
        self.assertEqual(self.queue(), [])

    def test_timeouts(self):
        #B: Job is not rescheduled if it times out.
        self.bike.set('job:Z', 'CONTINUE')
        self.assertEqual(self.queue(name='Z'), ['job:Z'])
        self.work_round()
        self.assertFalse(self.bike.is_working('job:Z'))
        self.assertEqual(self.queue(name='Z'), [])
