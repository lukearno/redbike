from itertools import cycle
import sys

from redbike import Redbike, RoundRobin


class Worker(RoundRobin):

    def queue_names(self, bike):
        return ['%s-work-%s' % (bike.prefix, x)
                for x in xrange(int(self.initstring))]

    def work(self, bike, jobid):
        bike.log.debug(jobid)


if __name__ == '__main__':

    JOBS = int(sys.argv[1])
    QUEUES = sys.argv[2]
    bike = Redbike(Worker(QUEUES), prefix='redbike-benchmark')
    bike.flush()
    queues = cycle(xrange(int(QUEUES)))
    for i in range(int(JOBS)):
        # bike.set('A:%s:%s' % (i, queues.next()),
        #          'DTSTART:20131009T164510\nRRULE:FREQ=MINUTELY')
        bike.set('B:%s:%s' % (i, queues.next()), 'CONTINUE')
