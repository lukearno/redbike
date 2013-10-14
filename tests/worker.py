
from redbike import RoundRobbin


class Worker(RoundRobbin):

    def work(self, bike, jobid):
        bike.log.debug(jobid)
