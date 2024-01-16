

import directorybatching.core.misc as misc
from collections import namedtuple
from abc import ABC, abstractmethod
from types import SimpleNamespace
import os
import logging
#Map = namedtuple("JobMap", "name param")

class Map:
    
    @property
    def name(self): return self._name

    @property
    def param(self): return self._param

    def __init__(self, name, param):
        self._name = name
        self._param = param



class Job(ABC):

    
    @property
    def flag_fpath(self):
        return os.path.join(self._out_dpath, ".job_COMPLETE")

    @property
    def is_flag_file(self):
        return os.path.isfile(self.flag_fpath)
    
    def mk_flag_file(self):
        open(self.flag_fpath, 'a').close()

    @property
    def log_name(self):
        return "job_%s" % self._leaf_id
        
    @property
    def logger(self):
        return logging.getLogger(self.log_name)

    def __init__(self, leaf_id, dpath, params, args, maps, mlog, plogger, out_dname='postprocessing'):
        self._leaf_id = leaf_id
        self._dpath   = dpath
        self._params  = params
        self._args    = args
        self._maps    = maps

        self._out_dpath = self._dpath if out_dname is None else misc.create_subdir(self._dpath, out_dname)
        self._log_fpath = log_fpath = os.path.join(self._out_dpath, 'log.txt')

        self._files = {}
        self._new_params = {}
        self._new_files = {}

        lname = self.log_name
        is_refresh = args.refresh and os.path.isfile(log_fpath)
        if is_refresh: os.remove(log_fpath)
        logger = mlog.new(lname, self._log_fpath, plogger, is_ignore=True)

        if is_refresh: logger.warning("Starting in refresh mode, old log deleted.")

    @property
    def dpath(self): return self._dpath

    @property
    def args(self): return self._args

    @property
    def params(self): return self._params

    @property
    def out_dpath(self): return self._out_dpath

    @abstractmethod
    def validate(self, fpath, params): pass


    def add_param(self, name, value):
        if name in self._params or name in self._new_params:
            self.logger.config("Already added parameter with name '%s' with value '%s'." % (name, value), is_force=True) 

        self._new_params[name]=value

    def add_file(self, name, fpath):
        if name in self._files or name in self._new_files:
            self.logger.config("Already added file with name '%s' at path '%s'." % (name, fpath), is_force=True) 

        self._new_files[name]=fpath

    @abstractmethod
    def execute(self): pass 

    def prep_return(self, status, msg=None, is_continue=False):
        rtnval = SimpleNamespace(leaf_id     = self._leaf_id   ,
                                 dpath       = self.dpath      ,
                                 status      = status          ,
                                 is_continue = is_continue     ,
                                 job_params  = self._new_params,
                                 job_files   = self._new_files ,
                                 msg         = msg             )

        self._new_params = {} 
        self._new_files = {}

        return rtnval
                

class MultiJob(ABC):
    def execute(self, fpath, params, args): pass

    @abstractmethod
    def get_job_list(self): pass
