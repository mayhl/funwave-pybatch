
from collections import namedtuple
from abc import ABC


Map = namedtuple("ParamMap", "name param_name")


class Param(ABC):


    def read(self, **kwargs):
        write("Param")

    def __init__(self, **kwargs):

        self.read(kwargs)
        


