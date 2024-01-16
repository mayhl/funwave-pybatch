#from directorybatching.model import FunwaveBatch
from directorybatching.model import FunwaveJob, FunwaveBatch
from directorybatching.core.table import Map as TableMap
from directorybatching.core.directory import Map as DirectoryMap
from directorybatching.core.job import Map as JobMap
import directorybatching.core.status as status
import directorybatching.core.parser as parser 
import argparse
import os 
from types import SimpleNamespace


##############################################################
# Parsers for converting between direction name and its
# associated matching string and value
##############################################################
# Validates that the directory name starts with some string
# and removes starting string for futher parsing 
validator  = parser.StartsWith()
# String of numbers is an integer
par_period = parser.Integer(validator)
par_depth  = parser.Integer(validator)
# String of numbers with 2 decimal places
# e.g. "032" <=> 0.32
par_relH   = parser.Decimal(validator, 2)
par_relF   = parser.Decimal(validator, 2, is_flip_sign=True)
# Also flipping sign 
# e.g. '1.26' <=> -1.26
par_relB   = parser.Decimal(validator, 2)
par_m      = parser.Integer(validator)
# Preporocessing step remove and store job id suffix
pre_jobid  = parser.Preprocessor.JobID()
par_cd     = parser.Decimal(validator, 3, preprocessor=pre_jobid)


class MyFunwaveBatch(FunwaveBatch):

    # Maps between internal name, directory match string, and parser
    def construct_dir_maps(self):
        maps = [DirectoryMap('period', 'period', par_period),
                DirectoryMap('depth' , 'depth' , par_depth ),
                DirectoryMap('relH'  , 'relH'  , par_relH  ),
                DirectoryMap('relF'  , 'relF'  , par_relF  ),
                DirectoryMap('relB'  , 'relB'  , par_relB  ),
                DirectoryMap('m'     , 'm'     , par_m     ),
                DirectoryMap('cd'    , 'cd'    , par_cd    )]

        return maps

    # Maps between internal name and table column heading/name 
    def construct_table_maps(self):
        maps = [TableMap('period', 'T'   ),
                TableMap('depth' , 'd'   ),
                TableMap("relH"  , r"H/h"),
                TableMap("relF"  , r"F/H"),
                TableMap("relB"  , r"B/L"),
                TableMap("m"     , "m"   ),
                TableMap("cd"    , "cd"  )]
 
        return maps

    # Maps between internal name and input.txt name
    def construct_job_maps(self):
        maps = [JobMap('dx'    , 'DX'    ),
                JobMap('CFL'   , 'CFL'   ),
                JobMap('mglob' , "Mglob" ),
                JobMap('dep_wk', "DEP_WK")]

        return maps

    # Optional method if you want to add you own command line arguments
    #def parse_cmd_args(self, parser):
    #
    #    parser.add_argument('-i', '--index', help='which file index to plot (integer)',
    #                     type=int)
    #    return parser

###################################################
# Example of custom states for sorting aggregated #
###################################################
class MyStatus(status.Base):

    CHECK_1  = status.Tuple(1, "Example failed check 1 message")
    CHECK_2 = status.Tuple(2, "Example failed check 2 message")
    SUCCESS = status.Tuple(3, "Example success message")

    # Required function for defining status(es) that is/are valid 
    def _is_valid(self): return self == MyStatus.SUCCESS


# Example postprocessing job 
class MyFunwaveJob(FunwaveJob):

    def execute(self):
        # Path to simulations
        dpath = self.dpath

        # Path to folder containing output data and plots
        #    default: dpath/postprocessng
        out_dpath = self.out_dpath

        # Dictionary of values read in maps, e.g., params['dx']
        params = self.params
        # Trick to access using dot notation, e.g., params.dx
        # params = SimpleNamespace(**(self.params))

        # Arguments from commands line (argparse)
        args = self._args

        # [Optional] logger (see logging)
        logger = self.logger

        is_check_1 = check_1(params)
        # Format return values in a standard way for aggregation
        if not is_check_1: 
            logger.error("Failed Check 1")
            return self.prep_return(MyStatus.CHECK_1) 

        is_check_2, msg = check_2(params)
        if not is_check_2: 
            logger.error("Failed Check 2")
            return self.prep_return(MyStatus.CHECK_2, msg=msg)

        # Example of adding some new value to aggregated data 
        new_val = some_function(dpath, params)
        self.add_param("new_val", new_val)

        # Example of adding some file to aggregated data
        some_plot_fpath = some_plot(dpath, out_dpath, params)
        self.add_file("some_plot", some_plot_fpath)

        return self.prep_return(MyStatus.SUCCESS)

# Example of some check on params 
def check_1(params):
    return True

# Example returning addition info about the failed check
def check_2(params):
    #return False, "Additional Info"
    return True, None

# Some postprocessing function
def some_function(dpath, params):
    return 0.5332

# Some example plotting routine
def some_plot(dpath, out_dpath, params):
    plot_file_path = os.path.join(out_dpath, "some_plot.png")
    return plot_file_path
    

# Running batch job
if __name__ == "__main__":
    MyFunwaveBatch(MyFunwaveJob).run()
    

