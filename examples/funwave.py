from directorybatching.model import FunwaveBatch
from directorybatching.core.table import Map as TableMap
from directorybatching.core.directory import Map as DirectoryMap
from directorybatching.core.param import Map as ParamMap
import directorybatching.core.parser as parser 
import argparse
import logging

validator  = parser.StartsWith()
par_period = parser.Integer(validator)
par_depth  = parser.Integer(validator)
par_relH   = parser.Decimal(validator, 3)
par_relF   = parser.Decimal(validator, 3, is_flip_sign=True)
par_relB   = parser.Decimal(validator, 3)
par_m      = parser.Integer(validator)
pre_jobid  = parser.Preprocessor.JobID()
par_cd     = parser.Decimal(validator, 4, preprocessor=pre_jobid)

class MyFunwaveBatch(FunwaveBatch):

    def process_sim(self, params, args):
        print("Test Run")

    # FUTURE IDEA: Add support read structure from files
    #              e.g. seperate files for parsers & structure
    def construct_dir_maps(self):

        maps = [DirectoryMap('period', 'period', par_period),
                DirectoryMap('depth' , 'depth' , par_depth ),
                DirectoryMap('relH'  , 'relH'  , par_relH  ),
                DirectoryMap('relF'  , 'relF'  , par_relF  ),
                DirectoryMap('relB'  , 'relB'  , par_relB  ),
                DirectoryMap('m'     , 'm'     , par_m     ),
                DirectoryMap('cd'    , 'cd'    , par_cd    )]

        return maps

    def construct_table_maps(self):
        
        maps = [TableMap('period', 'T'   ),
                TableMap('depth' , 'd'   ),
                TableMap("relH"  , r"H/h"),
                TableMap("relF"  , r"F/H"),
                TableMap("relB"  , r"B/L"),
                TableMap("m"     , "m"   ),
                TableMap("cd"    , "cd"  )]

    
        return maps

    def construct_param_maps(self):

        maps = [ParamMap('dx'   , 'DX'   ),
                ParamMap('CFL'  , 'CFL'  ),
                ParamMap('mglob', "Mglob")]

        return maps


    def parse_cmd_args2(self, parser):


        """Parse the commend line and assign appropriate argument values - will come from HPC Portal GUI"""
        parser.add_argument('-i', '--index', help='which file index to plot (integer)',
                         type=int)
        return parser



if __name__ == "__main__":

    log_lvl = 0
    #log_lvl = logging.INFO
    #parser=None
    funbatch = MyFunwaveBatch()
    funbatch.run()
    #args = parser.parse_args()
    

