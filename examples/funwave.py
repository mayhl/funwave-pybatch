
from directorybatching.model import FunwaveBatch
from directorybatching.core.table import Map as TableMap
from directorybatching.core.directory import Map as DirectoryMap
from directorybatching.core.param import Map as ParamMap
import argparse
import logging


# Parsing functions for directory names

# Strips away name from the start of a string, parsing the value
# func and return in dict, e.g.,
#   "name_431" => parser("_431") => {'value': 431}
def strip_name(name, string, parser):
    # Checking directory is valid
    if not string.startswith(name): return None
    val_str = string[len(name):]
    return {'value': parser(val_str)}


def fmt_int(name, string): return strip_name(name, string, int)

def fmt_3d(name, string): return strip_name(name, string, lambda n: int(n)/100)
def fmt_n3d(name, string): return strip_name(name, string, lambda n: -int(n)/100)

# Strips away last part of string seperated by a delimiter and passes
# begining to parsers to get name and value, e.g.,
#   "name_431_XY" => ("name_431", "XY") => {'value': 431, suffix: 'XY"}
def split_pop_last(name, string, nparser, vparser, delimiter='_'):

    comps = string.split(delimiter)

    if len(comps) < 2: return None, None

    *comps, suffix = comps
    string = delimiter.join(comps)

    rtn_dict = nparser(name, string, vparser)

    if rtn_dict is None: return None
    rtn_dict['suffix']=suffix

    return rtn_dict

def fmt_4d(name, string): 
    return split_pop_last(name, string, strip_name, lambda n: int(n)/1000)   
    


class MyFunwaveBatch(FunwaveBatch):

    def process_sim(self, params, args):
        print("Test Run")

    # FUTURE IDEA: Add support read structure from files
    #              e.g. seperate files for parsers & structure
    def construct_dir_maps(self):

        maps = [DirectoryMap('period', 'period', fmt_int, int  ),
                DirectoryMap('depth' , 'depth' , fmt_int, int  ),
                DirectoryMap('relH'  , 'relH'  , fmt_3d , float),
                DirectoryMap('relF'  , 'relF'  , fmt_n3d, float),
                DirectoryMap('relB'  , 'relB'  , fmt_3d , float),
                DirectoryMap('m'     , 'm'     , fmt_int, int  ),
                DirectoryMap('cd'    , 'cd'    , fmt_4d , float)]

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
    

