
from directorybatching.core.batch import Batch
from directorybatching.core.param import Param

class FunwaveParam(Param):
    pass

class FunwaveBatch(Batch):

    def get_param_type(self):
        return FunwaveParam

    def parse_cmd_args(self, parser):
        return parser

    def suffix_parser(self, val):
        *parts, job_id = val.split('_')
        
        new_val = '_'.join(parts)

        return new_val, job_id



        
