
import directorybatching.core.directory as directory

from directorybatching.core.parallel import simple as eparallel
import directorybatching.core.table as table
import directorybatching.core.job as job
import directorybatching.core.status as status
import directorybatching.core.misc as misc
from directorybatching.core.logger import MultiLogger, BufferedLogger

import argparse
import os
from abc import ABC, abstractmethod
from glob import glob
import numpy as np
import logging
from types import SimpleNamespace
import multiprocessing

class Batch(ABC):

    ##########################################
    # Methods to overwrite in derivied class #
    ##########################################
   
    #@abstractmethod
    #def validate(self, params, args): pass

    # Defines: 
    #    1) common nested directory structure 
    #    2) starting name of sub folders 
    #    3) parser for extacting a value from the folder name after
    #       remove name, e.g., "name_val" => parser("_val") => val
    @abstractmethod
    def construct_dir_maps(self): pass

    ################################################
    # Optional methods overwrite in derivied class #
    ################################################

    # Parser for seperating suffix from job directory name, 
    # e.g., param_value_jobid => param_value 
    def suffix_parser(self, val): return val, None

    # Addtional command line inputs
    def parse_cmd_args(self, parser): return parser

    # Defines mapping between table columns and internal parameters  
    # Returns a list of namedtuples 
    def construct_table_maps(self): return None

    # Defines mapping between internal parameters and parameters 
    # read at job path, e.g., model input parameters 
    def construct_param_maps(self): return None

    def get_job_type(self): return None


    ####################
    # Property Members #
    ####################

    @property
    def logger(self): return self._logger

    @property
    def fpaths(self): return self._fpaths

    @property
    def dpaths(self): return self._dpaths


    ########################
    #    Public methods    #
    ########################

    def _update_jobs(self, jobs, results, name):

        logger = self.logger

        for j, r in zip(jobs, results):
            j._params.update(r.job_params)
            j._files.update(r.job_files)

        self._table.update_from_jobs(jobs, results)
        jobs = [j for j, r in zip(jobs, results) if r.is_continue]

        self._dstruc.save_to_file()
        all_errors = [r.status for r in results if not r.status.is_valid]

        errors = []
        for e in all_errors:
            if not e in errors: errors.append(e)

        n = len(results)
        for e in errors:
            ne = np.sum(all_errors==e)
            if ne == 0: continue
            
            p = 100*ne/n
            logger.warning("%d/%d [%.1f%%] jobs failed %s with status '%s'." % (ne, n, p, name, e.display_string)) 

        is_no_error = len(errors) == 0
        is_no_jobs = len(jobs) == 0

        if is_no_jobs and not is_no_error:
            logger.error("No jobs passed %s" % name)

        self._is_no_jobs = is_no_jobs 
        if is_no_jobs:
            logger.banner("All Jobs Completed")

        if is_no_error:
            logger.info("All jobs passed %s" % name)
        else:
            nj = len(jobs)
            p = 100*nj/n
            logger.warning("%d/%d [%.1f%%] jobs passed %s" % (nj, nj, p, name))

        return jobs

    def run(self):

        logger = self.logger
        n_procs = self._args.num_procs

        logger.banner("Validating Jobs")
        list_args = self._table.prep_list_job_args()
        jobs = [self._jtype(*args, self._args, self._jmaps, self._mlogs, self._logger) for args in list_args]

        def log_callback(rtnval):

            msg = rtnval.status.name
            logger.info("Job %s [%s] path: %s" % (rtnval.leaf_id, msg, rtnval.dpath))

        results = eparallel([j.validate for j in jobs], n_procs, 'Validating', callback=log_callback)
        jobs = self._update_jobs(jobs, results, 'validation')

        logger.banner("Executing Jobs")
        results = eparallel([j.execute for j in jobs], n_procs, 'Executing ', callback=log_callback)
        jobs = self._update_jobs(jobs, results, self._name)


    def __init_directory(self):

        DNAMES = SimpleNamespace(
                    BATCH    = 'batch_postprocessing',
                    LOGS     = 'logs'                ,
                    BACKLOGS = 'backlogs'             )

        # Hacky check to see if is_suffix_parser method has been overwritten
        # Note: Assumes error is due to test input being invalid for overwritten method 
        try:
            val, suffix = self.suffix_parser("DUMMY")
            self._is_suffix_parser = not suffix == None
        except Exception as e:
            self._is_suffix_parser = True

        root_dpath = self._args.root_path

        out_dpath = misc.create_subdir(root_dpath, DNAMES.BATCH)

        logs_dpath = misc.create_subdir(out_dpath, DNAMES.LOGS)
        backlogs_dpath = misc.create_subdir(out_dpath, DNAMES.BACKLOGS)

        name = "main" if self._name is None else self._name
        log_fpath = os.path.join(logs_dpath, '%s.log' % name)

        dpaths = SimpleNamespace(
                    root    = root_dpath    ,
                    out     = out_dpath     ,
                    logs    = logs_dpath    ,
                    backlog = backlogs_dpath)

        fpaths = SimpleNamespace(log = log_fpath)

        return dpaths, fpaths

    def __init_logger(self, mlog, lvl): 

        if mlog is None: 
            mlog = MultiLogger(self.dpaths.logs, self.dpaths.backlog, self._is_refresh, self._is_backlog, lvl)
        else:
            t_mlog = type(mlog)
            if not t_mlog is MultiLogger: 
                self.logger.critical("Expected type MultiLogger for input multi_logger, got type '%s'." % t_mlog, TypeError)

        # Note: Need to give name to avoid all logs writting to root logger
        name = "MAIN" if self._name is None else self._name 
        logger = mlog.new(name, self._fpaths.log, self._logger)

        return logger, mlog

    def __init__(self, job_class, name=None, multi_logger=None):

        self._name = name
        self._args = self.__parse_batch_cmd_args() 

        self._is_refresh = self._args.refresh
        self._is_backlog = not self._args.no_backup
        log_lvl = self._args.log_level

        # Temporary logger until file path is estabilished 
        self._logger = BufferedLogger.getLogger(lvl=log_lvl) if multi_logger is None else multi_logger.logger
        
        #####################################
        self._logger.banner("Preinitialization")
        #####################################
        self.__check_inherited_method('parse_cmd_args', Batch)
        self._dpaths, self._fpaths = self.__init_directory()
        self._logger, self._mlogs = self.__init_logger(multi_logger, log_lvl)

        # Correct logger has estabilished 
        logger = self._logger
        logger.info("Preinitialization complete.")

        ###############################################
        logger.banner("Validating & Constructing Maps")
        ###############################################
        self._status_maps = status.Chained()
        self._dmaps = self.__construct_dir_maps()
        self._tmaps, self._is_tmaps = self.__construct_table_maps()
        self._jmaps, self._jtype, self._is_jmaps = self.__construct_job_maps(job_class)

        self.__check_dir_table_maps()
        logger.info("Map validated and constructed.")

        #####################################################
        logger.banner("Crawling & Validating Root Directory")
        #####################################################
        self._dstruc = directory.Structure(self)
        logger.info("Root directory crawled and validated")
        self._table = table.Table(self)

        # Syncing directory stucture to table  data
        if self._is_tmaps: self._table.sync_table_maps(self)

    
    ##########################
    #    Internal Methods    #
    ##########################

    # Required arugments for batching
    def __parse_batch_cmd_args(self):
  
        parser = argparse.ArgumentParser()
        
        parser.add_argument('-np', '--num-procs', type=int, default = 1,
                            help="Number of processors, default: %d" % 1)
        parser.add_argument('-rp', '--root-path', type=str,
                            help="Path to root directory of subdirectories to process")
        parser.add_argument('-op', '--output-path', type=str, default=None,
                            help="Path to directory for aggregated simulations outputs.")
        parser.add_argument('-ll', '--log-level', type=int, default=logging.INFO, 
                            help="Level of logging. Default: %d (%s)." % (logging.INFO, "logging.INFO"))
        parser.add_argument('--refresh', action='store_true',
                            help="Backup and start fresh run and re-run completed jobs.")
        parser.add_argument('--no-backup', action='store_true',
                            help="Turn off backup feature.")


        # Note: Better solution?
        is_tmap = not self.construct_table_maps() is None
        if is_tmap:
            parser.add_argument('-tp', '--table-path', type=str,
                                help="Path to support table." )

        return self.parse_cmd_args(parser).parse_args()

    def __check_inherited_method(self, meth_name, base_cls, is_restricted=False):

        logger = self.logger

        cname, ccls, irel = misc.parse_calling_child_class(self, meth_name, base_cls)
        sname = misc.get_class_fullname(type(self))

        if irel.is_base: 
            if is_restricted:
                logger.error("Method '%s' needs to be implemented in class derived from '%s'." % (meth_name, cname))
            else:
                logger.info("Method '%s' not implemented in derived class '%s'." % (meth_name, sname))
        elif irel.is_self:
            if is_restricted:
                logger.error("Method '%s' not implemented correctly in derived class '%s', method returns None." % (meth_name, cname))
        elif irel.is_intermediary:
            logger.warning("Method '%s' is being called from the intermediary derived class '%s'." % (meth_name, cname))  
            # NOTE: Required?
            #if is_restricted:
             #   logger.error("Method '%s' not implemented correctly in derived class '%s', method returns None." % (meth_name, cname))
        else:
            logger.critical("Method '%s' is in an expected state." % meth_name)

        return cname, ccls, irel, sname
    
    def __check_maps_wrapper(self, meth_name, map_type, is_required=True):

        logger = self.logger

        maps = getattr(self, meth_name)()

        is_map = not maps is None

        # Flag to throw error if maps is expected to be not None
        is_restricted = is_required and not is_map
        cname, ccls, irel, sname = self.__check_inherited_method(meth_name, Batch, is_restricted)
       
        # Assumes previous method throws if not None maps is required
        if not is_map: return None, False 

        # Validate maps
        map_name = misc.get_class_fullname(map_type)    
        if not type(maps) is list:
            logger.error("Method '%s' did not return a list in derived class '%s'." % (meth_name, cname))

        for i, item in enumerate(maps):
            itype = type(item)
            iname = misc.get_class_fullname(itype) 
            if not itype is map_type:
                logger.error("Method '%s' did not return a list of '%s' in derived class '%s'. Element %d is type '%s'!" % (meth_name, map_name, cname, i, iname))
        
        return maps, True

    def __construct_dir_maps(self):
        # Directory map is always defined, i.e, 2nd argument is always True 
        map, _ = self.__check_maps_wrapper('construct_dir_maps', directory.Map)
        return map 

    def __construct_table_maps(self):
        return self.__check_maps_wrapper('construct_table_maps', table.Map, is_required=False)

    def __construct_job_maps(self, jtype):

        logger = self.logger
        jmaps, is_jmaps = self.__check_maps_wrapper('construct_job_maps', job.Map, is_required=False)
        if not is_jmaps: return None, None, False

        # Strict validating of get_job_type method if construct_job_maps is valid
        # FIX LOGIC
        #cname, ccls, irel, sname = self.__check_inherited_method('get_job_type', Batch, is_restricted=False)   
        #jtype = self.get_job_type()

        #if jtype is None:
        #    logger.error("Method 'get_job_type' not implemented correctly in derived class '%s', returns None." % cname)

        if not issubclass(jtype, job.Job):
            pname = misc.get_class_fullname(job.Job)
            rname = misc.get_class_fullname(jtype)
            logger.error("Method 'get_job_type' does not return a class derived from '%s', got '%s'." % (pname, rname) )

        return jmaps, jtype, True


    def __check_map_compatability(self, omaps, otname):

        if omaps is None: return

        dnames = [m.name for m in self._dmaps]
        onames = [m.name for m in omaps]

        for oname in onames:
            if not oname in dnames:
                self.logger.error("No matching name '%s' from '%s' in 'DirectoryMaps'." % (oname, otname)) 

    def __check_dir_table_maps(self):
        self.__check_map_compatability(self._tmaps, 'TableMaps')








        
