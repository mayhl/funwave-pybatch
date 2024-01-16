import io
import sys
import os
from glob import glob
import logging
from datetime import datetime
import tarfile
import shutil
import collections


MULTI_LOGGER_FORMAT = '[%(asctime)s] (%(levelname)s) %(message)s'

# Custom log levels, see method addLoggingLevel at end of file
#   BANNER - prettified 3 line banner for sectioning that alway show
#   CONFIG - User error in configuring batch
CUSTOM_LOG_LEVELS = [("BANNER",  49),
                     ("CONFIG",  45)]

class ConfigError(Exception):
    "Error if there is an issue with the configuration of the batch job"
    pass

# Simple base Logger class with exceptions and banner feature  
class BaseLogger(logging.RootLogger):

    @classmethod
    def fmt_critical(cls, msg):
        return "(CRITICAL) %s Please contact developer." % msg

    def critical(self, msg, etype=Exception, is_force=False): 
        super().critical(msg)
        if self._is_except or is_force: raise etype(BaseLogger.fmt_critical(msg))

    def error(self, msg, etype=Exception, is_force=False):
        super().error(msg)
        if self._is_except or is_force: raise etype(msg)

    def config(self, msg):
        super().config(msg, is_force=False)
        if self._is_except or is_force: raise ConfigError(msg)

    def banner(self, title):
        
        MAX_LENGTH=40
        
        n = len(title)
        if n <= MAX_LENGTH:
            s = (MAX_LENGTH-n)//2
            n = MAX_LENGTH
        else:
            s = 0
            n = MAX_LENGTH

        title = ''.join([' ']*s) + title 
        banner = ''.join(['=']*n)

        for m in [banner, title, banner]: super().banner(m)
    
    @classmethod
    def getLogger(cls, name, lvl=logging.INFO, is_ignore=False, fpath=None):
        logger = logging.getLogger(name)
        logger.__class__ = BaseLogger
        logger._is_except = not is_ignore
        if not fpath is None:
            logger._fpath = fpath

        #### WERID BUG ###############################################
        # Log level 0 with custom handler behaves like log level 30. #
        # Does not seem to be an issues with other log levels.       # 
        # Hacky solution until bug source idenitified                # 
        if lvl == 0: lvl = -1                                        #
        logger.setLevel(lvl)                                         #
        ##############################################################

        return logger 

# Logger to setup different files 
class FileLogger(BaseLogger):

    @classmethod
    def getLogger(cls, name, fpath, lvl=logging.INFO, is_ignore=False, fmt=None):

        if fmt is None: fmt = MULTI_LOGGER_FORMAT

        # Casting logger 
        logger = super().getLogger(name, is_ignore=is_ignore, lvl=lvl, fpath=fpath)
        logger._class__ = FileLogger

        # Configuring logger
        handler = logging.FileHandler(fpath)    
        formatter = logging.Formatter(fmt)

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        #logger.setLevel(lvl)

        return logger


# Last In First Out (LIFO), or First In Last Out (FILO) 
# list handler for storing logging messages
class LIFOHandler(logging.Handler):

    def __init__(self, log_queue):
        logging.Handler.__init__(self)
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.append(self.format(record))


# Logger to temporarily store logs until file path is estabished for true logger 
class BufferedLogger(BaseLogger):

    # Wrapper method to flush buffer logs to stdout before raising exception
    def __flush_exception(self, func, msg, etype):
        try:
            func(msg, etype)
        except Exception as e:
            self.flush()
            raise e

    def critical(self, msg, etype): self.__flush_exception(super().critical, msg, etype)
    def error(self, msg, etype): self.__flush_exception(super().error, msg, etype)
    def config(self, msg): self_flush_exception(super().config, msg)

    def flush(self, logger=None):

        # Defaulting to stdout if no logger provided, 
        # e.g., dump to stdout if error before path setup
        stream = sys.stdout if logger is None else open(logger._fpath, 'a')
        for line in self._buffer: stream.write(line + "\n")
        self._buffer = []

    @classmethod
    def getLogger(cls, name="BUFFERED_LOGGER", lvl=logging.INFO, fmt=None):

        if fmt is None: fmt = MULTI_LOGGER_FORMAT

        # 'Casting'
        logger = super().getLogger(name, lvl=lvl)        
        logger.__class__ = BufferedLogger
        logger._buffer = []

        # Configuring logger
        handler = LIFOHandler(logger._buffer)
        formatter = logging.Formatter(fmt)

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

class MultiLogger:

    def __init__(self, dpath, back_dpath, is_refresh, is_backlog, log_lvl=logging.INFO):

        self._dpath = dpath
        self._handler = None
        self._logs = {}
        self._log_lvl = log_lvl

        self._dpath = dpath
        self._back_dpath = back_dpath

        self._is_refresh = is_refresh
        self._is_backlog = is_backlog
        self._logger = None


    @property
    def logger(self): return self._logger

    def get(name):
        if not name in self._logs: 
            self._logger.error("Can not get logger with name '%' as it has not been created!" % name)
        return self._logs[name]
    
    def new(self, name, fpath, parent_logger, lvl=None, is_ignore=False, fmt=None):

        if fmt is None: fmt = MULTI_LOGGER_FORMAT
        if lvl is None: lvl = self._log_lvl

        tlog = type(parent_logger)
        if not issubclass(tlog, BaseLogger):
            msg ="parent_logger must be type BaseLogger or one of it's derived types: got type '%s." % tlog
            raise TypeError(BaseLogger.fmt_critical(msg)) 

        is_first = len(self._logs) == 0
        is_blog = tlog is BufferedLogger
        if is_first and not is_blog:
            parent_logger.critical("Expected type BufferedLogger as parent_logger for first new log.", TypeError)

        is_new = False if not is_first else self.__init_directory(parent_logger)

        if name in self._logs: parent_logger.critical("Logger with name '%s' has already been created!" % name)

        parent_logger.debug("Creating logger '%s' at path '%s'." % (name, fpath))
        logger = FileLogger.getLogger(name, fpath, lvl=lvl, fmt=fmt, is_ignore=is_ignore)

        self._logs[name] = logger

        if is_new and is_blog: 
            parent_logger.flush(logger)
            del parent_logger
            logger.debug("Flushed from Buffered Logger")
        
        if is_new and not is_blog:
            #############################################################
            raise NotImplementedError("MULTI BATCH MODE NOT IMPLEMENTED")
            #############################################################

        return logger


    def __init_directory(self, parent_logger):

        logger = parent_logger

        dpath = self._dpath
        n = len(os.listdir(dpath))
        is_new = n == 0 

        if is_new: 
            logger.info("Starting new run.")
            return True

        if not self._is_refresh: 
            logger.info("Continuing from previous run.")
            ###########################################################
            raise NotImplementedError("CONTINUE MODE NOTE IMPLEMENTED")
            ###########################################################
            return False

        logger.info ("Starting in refresh mode")

        if not self._is_backlog:
            logger.warning("Backup mode turned off manually, old logs will be lost.")
        else:

            logger.info("Backing up previous logs...")

            arc_fname = datetime.now().strftime("%Y%m%d%H%M.tar.gz")

            arc_fpath = os.path.join(self._back_dpath, arc_fname)

            if os.path.isfile(arc_fpath):
                raise logger.critical("EXISITING ARCHIVE FILE PATH RESOLUTION NOT IMPLEMENTED.", NotImplementedError)


            with tarfile.open(arc_fpath, "w:gz") as tar:
                tar.add(dpath, arcname=os.path.basename(dpath))

            logger.info("Previous logs back up to '%s'." % arc_fpath) 

        logger.info("Deleting old logs...")
        shutil.rmtree(dpath)
        os.mkdir(dpath) 
        logger.info("Old logs deleted")

        return True

# Code modified from Mad Physicist @ stackoverflow
# link: https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility
def addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present 

    Example
    -------
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
       raise AttributeError('{} already defined in logging module'.format(levelName))
    if hasattr(logging, methodName):
       raise AttributeError('{} already defined in logging module'.format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
       raise AttributeError('{} already defined in logger class'.format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)
    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)

# Creating logging levels defined at top 
for NAME, LEVEL in CUSTOM_LOG_LEVELS: addLoggingLevel(NAME, LEVEL)
