"log all print and stdout, stderr to files, import at the top of Run.py"
from __future__ import print_function
import builtins
import os, sys
from datetime import date, datetime
import logging
import multiprocessing

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, level):
       self.logger = logger
       self.level = level
       self.linebuf = ''

    def write(self, buf):
       for line in buf.rstrip().splitlines():
          self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass

loger_fn = os.path.join('.logs', "{:%y.%m.%d}.debug.log".format(datetime.now()))
# logfn = '.logs\\test.log'
log_fmt = "[%(asctime)s       :%(processName)s:%(levelname)s] %(message)s"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler(loger_fn, encoding='utf8')
fh.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter(fmt=log_fmt, datefmt='%H:%M:%S')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
# logger.addHandler(ch)

# def global_print(*args, **kwargs):
#     logger.debug(' '.join(str(x) for x in args))
# #sys.stdout = logger.info
# #sys.stderr = sys.stdout
# sys.stdout = StreamToLogger(logger,logging.INFO)
# sys.stderr = StreamToLogger(logger,logging.ERROR)
# builtins.print = global_print

class mylogger:
   def __init__(self, info_fn, debug_fn):
      self.info_fn = info_fn
      self.debug_fn = debug_fn
   def write(self, s):
      self.info(s)

   def info(self, s):
      ts =datetime.now()
      self.debug(s, ts=ts)
      pre = ts.strftime("[%X] ")
      msg = pre + s.replace('\n', '\n'+pre) + '\n'
      sys.__stdout__.write(msg)
      with open(self.info_fn, 'a+', newline='\n', encoding='utf8') as f:
         f.write(msg)

   def debug(self, s, ts=None):
      ts = ts or datetime.now()
      p = multiprocessing.current_process()
      pre = "[{0:%X.%f}:{1.name}] ".format(ts, p)
      msg = pre + s.replace('\n', '\n'+pre) + '\n'
      with open(self.debug_fn, 'a+', newline='\n', encoding='utf8') as f:
         f.write(msg)

   def flush(self):
      sys.__stdout__.flush()


# logfn = os.path.join('.logs', "{:%y.%m.%d %H.%M.%S} debug.log".format(datetime.now()))
infofn = os.path.join('.logs', "{:%y.%m.%d}.console.log".format(datetime.now()))
debugfn = os.path.join('.logs', "{:%y.%m.%d}.debug.log".format(datetime.now()))

global_logger = mylogger(infofn, debugfn)
def global_print(*args, debug=False, **kwargs):
   if debug:
      global_logger.debug(' '.join(str(x) for x in args))
   else:
      global_logger.info(' '.join(str(x) for x in args))

sys.stdout = global_logger
sys.stderr = global_logger
builtins.print = global_print

#exec(open("custom_logger.py").read())
