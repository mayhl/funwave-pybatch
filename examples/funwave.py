from directorybatching.model import FunwaveBatch
from directorybatching.core.table import Map as TableMap
from directorybatching.core.directory import Map as DirectoryMap
from directorybatching.core.param import Map as ParamMap
import argparse
import logging

# Parsing functions for directory names

# Strips away name from the start of a string, parsing the value, 
# and return a dict, e.g.,
#   "name431" => parser("431") => {'value': 431}
def startswith(name, string, parser):
    if not string.startswith(name): return None
    val_str = string[len(name):]
    return {'value': parser(val_str)}

# Last argument is a function, e.g., int(x)
def fmt_int(name, string): return startswith(name, string, int)

# NOTE: Lambda function, quick way define simple functions, e.g., y=f(x)
# Formatting a 3 digit string with decimal after first, e.g.,
#  '010' => 0.1, '212' => 2.12
def fmt_3d(name, string) : return startswith(name, string, lambda x:  int(x)/100)
# Same as before but numbers are negative, e.g.,
#  '527' => -5.27
def fmt_n3d(name, string): return startswith(name, string, lambda x: -int(x)/100)

# Using virtual keywords you can setup a virtual subdirectory splitting 
# at final directory depth, e.g., seperating simulations by job ID 
#
# Cuts away last part of string seperated by delimiter, passes
# result, to parsers, and adds virtual keyword/value pairs, e.g., 
#   "name_431_XY" => ("name_431", "XY") => 
#     {'value'        : 431         , 
#      'virtual_name' : virtual_name,
#      'virtual_value': 'XY'        }
def cut_last(name, string, virtual_name, nparser, vparser, delimiter='_'):

    comps = string.split(delimiter)

    if len(comps) < 2: return None, None

    *comps, virtual_value = comps
    string = delimiter.join(comps)

    rtn_dict = nparser(name, string, vparser)

    if rtn_dict is None: return None

    virtual_dict={'virtual_name' : virtual_name, 
                  'virtual_value': virtual_value}

    return {**rtn_dict, **virtual_dict}

# Wrapper function for above specifying the name of the virtual subdirectory 
def cut_jobid(name, string, nparser, vparser):
    return cut_last(name, string, 'jobid', nparser, vparser)

# Similar to before but with 4 digit number and spliting off jobid
def fmt_cd(name, string): 
    return cut_jobid(name, string, startswith, lambda n: int(n)/1000)   
    

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
                DirectoryMap('cd'    , 'cd'    , fmt_cd , float)]

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
    

