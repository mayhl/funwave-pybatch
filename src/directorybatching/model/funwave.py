

import directorybatching.core.status as status
from directorybatching.core.batch import Batch
from directorybatching.core.job import Job

import os
import numpy as np

class Status(status.Base):

    UNKNOWN    = status.Tuple(1 , "UNKNOWN STATE")
    NO_INPUT   = status.Tuple(2 , "No input.txt file")
    INPUT_FAIL = status.Tuple(3 , "Failed to read input.txt")
    NO_MATCH   = status.Tuple(4 , "Input.txt values do not match table "\
                                  "values and/or directory parse")
    NO_LOG     = status.Tuple(5 , "No LOG.txt file")
    LOG_FAIL   = status.Tuple(6 , "Failed to read LOG.txt file")
    UNSTABLE   = status.Tuple(7 , "Instability detected")
    NO_HPC     = status.Tuple(8 , "No HPC Output file")
    HPC_FAIL   = status.Tuple(9 , "Failed to read HPC output file")
    ABORTED    = status.Tuple(10, "HPC Aborted Job")
    WALLTIME   = status.Tuple(11, "Wall time reached output file")
    QUEUED     = status.Tuple(12, "In PBS queue" ) 
    RUNNING    = status.Tuple(13, "Running")
    FINISHED   = status.Tuple(14, "Completed")
    VALID      = status.Tuple(15, "Valid simulation")
    COMPLETED  = status.Tuple(16, "Previously Completed") 

    def _is_valid(self): return self == Status.VALID

class FunwaveBatch(Batch):
    def __init__(self, job_class,  name="FunwaveBatch",  multi_logger=None):
        super().__init__(job_class, name, multi_logger)

class FunwaveJob(Job):


    def validate(self):
        dpath = self._dpath

        logger = self.logger

        is_valid, rtnvals = self._validate_input(dpath)
        if not is_valid: return rtnvals  
        job_params = rtnvals

        is_continue, rtnvals = self._validate_log(dpath, job_params)
        if not is_continue: return rtnvals   

        is_continue, rtnvals = self._validate_hpc(dpath)
        if not is_continue: return rtnvals

        return self.prep_return(Status.UNKNOWN, job_params, "Reached end of checks")

    def _validate_input(self, dpath):

        fpath = os.path.join(dpath, 'input.txt')
        
        is_input = os.path.isfile(fpath)
        if not is_input: return False, self.prep_return(Status.NO_INPUT)

        try:
            iparams = read_input_file(fpath)
        except Exception as e:
            return False, self.prep_return(Status.INPUT_FAIL)

        # Checking and filtering in map values are in input file
        args = [(j.param in iparams, j) for j in self._maps]
        params = {j.name: iparams[j.param] for is_in, j in args if is_in}

        is_match = len(params) == len(args)
        if not is_match: 
            names = [j.param for is_in, j in args if not is_in]
            for k, v in params.items(): self.add_param(k ,v)
            msg = "Could not read input parameters %s." % names
            return False, self.prep_return(Status.NO_MATCH, msg)

        ##################################################
        #params['dep_wk'] = 2.5
        ##################################################

        diffs = []
        for j in self._maps:

            val_i = iparams[j.param]

            if not j.name in self._params:
                self.add_param(j.name, val_i)
                continue 

            val_s = self._params[j.name]
            if val_s == val_i: 
                self.add_param(j.name, val_i)
            else:
                self.add_param( "%s__INPUT__" % j.name, val_i)
                diffs.append((j.name, val_s, val_i))

        if len(diffs) > 0:
            msg = "%s" % diffs
            return False, self.prep_return(Status.NO_MATCH, msg)

        #print(params)
        return True, params 

    def _validate_log(self, dpath, params): 
        
        fpath = os.path.join(dpath, 'LOG.txt')
        is_log = os.path.isfile(fpath)

        if not is_log: return self.prep_return(Status.NO_LOG)

        strings = ["Normal Termination!", "PRINTING FILE NO. 99999"]
        
        try:
            matches = any_string_in_file(fpath, strings)
        except Exception as e:
            return False, self.prep_return(Status.LOG_FAIL)

        # Skipping to next validation methjod
        if np.sum(matches) == 0: return True, None

        if matches[0]:
            if self.is_flag_file:
                return False, self.prep_return(Status.COMPLETED)
            else:
                return False, self.prep_return(Status.VALID, is_continue=True)

        if matches[1]:
            msg = "%f" % params['CFL'] if 'CFL' in params else None
            return False, self.prep_return(Status.UNSTABLE, msg)


        raise Exception("Not all string matches handled correctly in _validate_log")

    def _validate_hpc(self, dpath):

        raise NotImplementedError()

        # Implement find HPC error 

        strings = ["application called MPI_Abort", "PBS: job killed: walltime"]
        
        try:
            matches = any_string_in_file(fpath, strings)
        except Exception as e:
            return False, self.prep_return(Status.HPC_FAIL)

        # Skipping to next validation methjod
        if np.sum(matches) == 0: return True, None

        if matches[0]:
            return False, self.prep_return(Status.ABORTED)

        if matches[1]:
            return False, self.prep_return(Status.WALLTIME)


        raise Exception("Not all string matches handled correctly in _validate_hpc")


def any_string_in_file(fpath, strings, is_reverse=True):

    if type(strings) is str: strings=[strings]

    with open(fpath) as f: lines = f.readlines()
    if is_reverse: lines = reversed(lines)

    is_found = False

    for line in lines:
        matches = [s in line for s in strings]
        if np.any(matches): return matches

    return False


def read_input_file(fpath):
    """
    Convert FUNWAVE input/driver file to dictionary

    :param fpath: Path to FUNWAVE input/driver file
    :type fpath: str
    """

    def _split_first(line, char):

        first, *second = line.split(char)
        second = char.join(second)
        return first, second

    def _filter_comment(line):

        if "!" not in line: return False, line, None
        first, second = _split_first(line, "!")
        return True, first, second


    with open(fpath, 'r') as fh: lines = fh.readlines()

    params = {}
    for line in lines:

        if not '=' in line: continue
        first, second = _split_first(line, "=")

        is_comment, name, _  = _filter_comment(first.strip())
        if is_comment: continue

        is_comment, val_str, _ = _filter_comment(second.strip())

        params[name] = parse_str(val_str)

    return params

def parse_str(val):

    def cast_type(val, cast):
        try:
            return cast(val)
        except ValueError:
            return None

    ival = cast_type(val, int)
    fval = cast_type(val, float)

    if ival is None and fval is None:
        if type(val) is str and len(val) == 1:
            if val[0] == 'T': return True
            if val[0] == 'F': return False

        return str(val)

    elif ival is not None and fval is not None:
        return ival if ival == fval else fval
    elif fval is not None: # and ival is None
        return fval
    else: # fval is None, ival is not None
        # Case should not be possible
        raise Exception('Unexpected State')
