"""redbike - A simple scheduler using Redis and iCal rrules.

Usage:
 redbike [--config=<CONF>] set <JOBID> <SCHEDULE> [--after=<TIMESTAMP>]
 redbike [--config=<CONF>] unset <JOBID>
 redbike [--config=<CONF>] dispatch [<WORKER>]
         [--schedules=<SCHEDULESCSV> [--after=<TIMESTAMP>]]
 redbike [--config=<CONF>] work [<WORKER>]
 redbike [--config=<CONF>] statuses [--before=<TIMESTAMP>]
 redbike [--config=<CONF>] schedules
 redbike [--config=<CONF>] tell <JOBID>
 redbike [--config=<CONF>] control <SIGNAL>

Arguments:
 <JOBID>        The id string of a job.
 <SCHEDULE>     The schedule for a job.
                Either RRule, CONTINUE, AT:<TIMESTAMP> for a one-off or STOP.
 <WORKER>       Worker instance. Overrides config. package.modeule:Worker('X')
 <SIGNAL>       Signal to dispatcher and worker processes. Currently only HALT.

Options:
 -a, --after=<TIMESTAMP>         Unix time.
 -b, --before=<TIMESTAMP>        Unix time.
 -s, --schedules=<SCHEDULESCSV>  CSV of JOBID, SCHEDULE pairs for startup.
 -c, --config=<CONF>             A config file with a [redbike] section.
"""

from ConfigParser import SafeConfigParser
import csv
import errno
import fcntl
import json
import os
import signal
import sys
import threading

import docopt
import resolver

from redbike import log, Redbike, __version__


# TODO: Validate all the inputs!


def do_set(bike, args):
    after = args['--after']
    jobid = args['<JOBID>']
    schedule = args['<SCHEDULE>']
    bike.set(jobid, schedule, after=after)


def do_unset(bike, args):
    jobid = args['<JOBID>']
    bike.unset(jobid)


def do_dispatch(bike, args):
    after = args['--after']
    csvfilename = args['--schedules']
    bike.clear_control()
    bike.dispatch(after=after, csvfilename=csvfilename)


def do_work(bike, args):
    bike.clear_control()
    bike.work()


def do_statuses(bike, args):
    before = args['--before']
    writer = csv.writer(sys.stdout)
    for jobid, event, timestamp in bike.get_statuses(before=before):
        writer.writerow([jobid, event, timestamp])


def do_schedules(bike, args):
    writer = csv.writer(sys.stdout)
    for jobid, schedule in bike.get_schedules():
        writer.writerow([jobid, schedule])


def do_tell(bike, args):
    jobid = args['<JOBID>']
    print json.dumps(bike.tell(jobid), indent=2)


def do_control(bike, args):
    bike.control(args['<SIGNAL>'])


_shutdown = False


def sig_handler(signum, frame):
    global _shutdown
    _shutdown = True


def run():
    args = docopt.docopt(__doc__, help=True, version=__version__)
    config_file = args['--config'] or '.redbike.conf'
    parser = SafeConfigParser()
    parser.read([config_file])
    conf = dict(parser.items('redbike'))
    try:
        redis_conf = dict(parser.items('redbike-redis'))
    except:
        redis_conf = {}
    command = [k for k, v in args.items() if v and k[0].isalpha()][0]
    stop_event = threading.Event()
    bike = Redbike(resolver.resolve(args['<WORKER>'] or conf['worker']),
                   prefix=conf.get('prefix'),
                   redis_config=redis_conf,
                   log=log,
                   timefile=conf.get('timefile'),
                   stop_event=stop_event,
                   default_timeout=conf.get('default-timeout', 10))
    func = globals()['do_' + command]
    pipe_r, pipe_w = os.pipe()
    flags = fcntl.fcntl(pipe_w, fcntl.F_GETFL, 0)
    flags |= os.O_NONBLOCK
    flags = fcntl.fcntl(pipe_w, fcntl.F_SETFL, flags)
    signal.set_wakeup_fd(pipe_w)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGHUP, sig_handler)
    signal.signal(signal.SIGQUIT, sig_handler)
    signal.signal(signal.SIGTSTP, sig_handler)

    def wrapper(bike, args):
        global _shutdown
        try:
            func(bike, args)
        finally:
            _shutdown = True
            os.write(pipe_w, '0')

    thd = threading.Thread(target=wrapper, args=(bike, args))
    thd.start()
    while not _shutdown:
        while True:
            try:
                os.read(pipe_r, 1)
                break
            except OSError, e:
                if e.errno != errno.EINTR:
                    stop_event.set()
                    raise
    stop_event.set()
