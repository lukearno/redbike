import logging
import pkg_resources
import sys

from redbike.schedule import Redbike, RoundRobin, StopWork, UnsetJob


version_file = pkg_resources.resource_filename(__name__, 'VERSION')
with open(version_file) as vf:
    __version__ = vf.read()
del version_file

LOGLEVEL = logging.DEBUG
FORMAT = '[%(asctime)s - %(module)20s - %(process)5d] %(message)s'
log = logging.getLogger('redbike')
log.setLevel(LOGLEVEL)
hdlr = logging.StreamHandler(sys.stdout)
hdlr.setFormatter(logging.Formatter(FORMAT))
log.addHandler(hdlr)


__all__ = ['log', 'Redbike', 'RoundRobin', 'StopWork', 'UnsetJob',
           '__version__']
