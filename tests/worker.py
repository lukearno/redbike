
from redbike import RoundRobin


class Worker(RoundRobin):

    def work(self, bike, jobid):
        bike.log.debug(jobid)
