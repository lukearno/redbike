import os
import time

from unittest import TestCase

from flexmock import flexmock

from redbike import Redbike, RoundRobbin, StopWork


class TestWorker(RoundRobbin):

    def work(self, bike, jobid):
        bike.redis.hincrby('biketest-results', jobid, amount=1)
        if jobid.startswith('stopper:'):
            raise StopWork("Stop this job.")
        if jobid.startswith('fail:'):
            raise Exception("Boom")


class RedbikeTests(TestCase):

    def setUp(self):
        self.bike = Redbike(TestWorker('A'), prefix='biketest')
        self.r = self.bike.redis  # for convenience
        self.bike.flush()
        self.r.delete('biketest-results')
        self.bike.control("HALT")

    def queue(self, name='A'):
        return self.r.lrange('biketest-work-%s' % name, 0, -1)

    def result(self, jobid):
        return self.r.hget('biketest-results', jobid)

    def timeline(self):
        return self.r.zrange(self.bike.timeline_key, 0, -1)

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
        self.bike.work()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(['job:A'], self.queue())
        #B: A CONTINUE job set to STOP is not requeued.
        self.bike.set('job:A', 'STOP')
        self.bike.work()
        self.assertEqual(self.result('job:A'), '2')
        self.assertEqual(self.queue(), [])

    def test_rrule(self):
        self.bike.set('job:A', "DTSTART:20131009T164510\nRRULE:FREQ=Secondly")
        tell = self.bike.tell('job:A')
        #B: Setting a job to RRULE puts a TML event in statuses.
        self.assertTrue(tell['status'].startswith('TML:'))
        #B: Setting a job to RRULE enters the job in the timeline.
        self.assertEqual(self.timeline(), ['job:A'])
        #B: Setting a job to RRULE does not go straight to the work queue.
        self.assertEqual(self.queue(), [])
        #B: Setting a job to RRULE is put in the work queue by the scheduler.
        self.bike.dispatch()
        self.assertEqual(self.queue(), ['job:A'])
        #B: Setting a job to RRULE goes back in the timeline after it is run.
        self.bike.work()
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
        self.bike.work()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(self.queue(), [])

    def test_at(self):
        self.bike.set('job:A', 'AT:%s' % int(time.time()))
        tell = self.bike.tell('job:A')
        #B: Setting a job to AT:TIMESTAMP puts a TML event in statuses.
        self.assertTrue(tell['status'].startswith('TML:'))
        #B: Setting a job to AT:TIMESTAMP enters it into the timeline.
        self.assertEqual(self.timeline(), ['job:A'])
        #B: Setting a job to AT:TIMESTAMP does not go straight to thequeue.
        self.assertEqual(self.queue(), [])
        #B: Setting a job to AT:TIMESTAMP is queued by the scheduler.
        self.bike.dispatch()
        self.assertEqual(self.queue(), ['job:A'])
        #B: An AT:TIMESTAMP job does not go back into the timeline or queue.
        self.bike.work()
        self.assertEqual(self.result('job:A'), '1')
        self.assertEqual(self.timeline(), [])
        self.assertEqual(self.queue(), [])

    def test_dispatch_with_csv(self):
        #B: Dispatching with a CSV of job,schedule records schedules the jobs.
        self.bike.dispatch(csvfilename='tests/test.csv')
        sched = dict(self.bike.get_schedules())
        self.assertEqual(sched['job1:A'], 'CONTINUE')
        self.assertEqual(sched['job2:B'], 'STOP')
        statuses = list(self.bike.get_statuses())
        self.assertEqual(len(statuses), 1)

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
        self.bike.work()
        self.assertEqual(self.result('stopper:A'), '1')
        self.assertEqual(self.bike.tell('stopper:A')['schedule'], 'STOP')
        #B: Raising StopWork cause the job to not run again.
        self.bike.work()
        self.assertEqual(self.result('stopper:A'), '1')

    def test_job_blows_up(self):
        self.bike.set('fail:A', 'CONTINUE')
        #B: Raising an unexpected error is handled.
        self.assertEqual(self.bike.tell('fail:A')['schedule'], 'CONTINUE')
        self.bike.work()
        self.assertEqual(self.result('fail:A'), '1')
        tell = self.bike.tell('fail:A')
        self.assertTrue(tell['status'].startswith('DIE'))
        self.assertEqual(self.bike.tell('fail:A')['schedule'], 'CONTINUE')
        #B: When an unexpected error is raise the job is not rescheduled.
        self.bike.work()
        self.assertEqual(self.result('fail:A'), '1')
