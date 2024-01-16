

from collections import namedtuple
import numpy as np
from abc import abstractmethod, ABC, ABCMeta
from enum import auto, Enum, EnumMeta


# Code thanks to martineau @ stackoverflow
# Link: https://stackoverflow.com/questions/56131308/create-an-abstract-enum-class
class ABCEnumMeta(ABCMeta, EnumMeta):

    def __new__(mcls, *args, **kw):
        abstract_enum_cls = super().__new__(mcls, *args, **kw)
        # Only check abstractions if members were defined.
        if abstract_enum_cls._member_map_:
            try:  # Handle existence of undefined abstract methods.
                absmethods = list(abstract_enum_cls.__abstractmethods__)
                if absmethods:
                    missing = ', '.join(f'{method!r}' for method in absmethods)
                    plural = 's' if len(absmethods) > 1 else ''
                    raise TypeError(
                       f"cannot instantiate abstract class {abstract_enum_cls.__name__!r}"
                       f" with abstract method{plural} {missing}")
            except AttributeError:
                pass
        return abstract_enum_cls

Tuple=namedtuple("Tuple", "value display_string")

class Base(Enum, metaclass=ABCEnumMeta):

    @property
    def display_string(self): return self.value.display_string

    @property
    def id(self): return self.value.value

    @property
    def is_valid(self): return self._is_valid()
    
    @abstractmethod
    def _is_valid(self): pass

    @classmethod
    def get_by_id(cls, id):

        fs = [s for s in cls if s.id == id]
        if len(fs) == 1: return fs[0]

        if len(fs) > 1: raise ValueError("More than one enum with same ID in '%s'" % cls)
        raise ValueError("ID %d is not in Enum '%s'." (id, cls))

    @classmethod
    def all_valid(cls, statuses): 
        return np.all([s.is_valid for s in statuses])

    @classmethod
    def any_valid(cls, statuses):
        return np.any([s.is_valid for s in statuses])


    @classmethod
    def max(cls, statuses):
        max_id = np.max([s.id for s in statuses])
        return cls.get_by_id(max_id)

    @classmethod
    def min(cls, statuses):
        min_id = np.min([s.id for s in statuses])
        return cls.get_by_id(min_id)


class Chained:

    def __init__(self):
        self._statuses = {}
    
    def _get_key(self, status):
        return status

    def get_id(self, status):
        key = self._get_key(type(status))
        offset = self._statuses[key]['id_start']
        return status.id + offset

    def get_status(self, id):

        for key in self._statuses:
            id_start = self._statuses[key]['id_start']
            id_end   = self._statuses[key]['id_end']
            if id_start <= id and id < id_end:
                return self._statuses[key]['class'].get_by_id(id-id_start)

        raise TypeError("ID %d can not be matched to a Chained status." % id )
    
    def has(self, status):
        return status in self._statuses

    def append(self, derived_type):

        if not issubclass(derived_type, Base):
            raise TypeError("Can only append Statuses derivied from base, got '%s'." % derived_type)

        if derived_type in self._statuses: raise Exception()

        ids = np.array([s.id for s in derived_type])
        id_max = ids.max()
        id_min = ids.min()

        if len(self._statuses) == 0:
            id_start = 0
        else:
            id_start = np.max([s['id_end'] for k, s in self._statuses.items()])

        id_start += id_min
        id_end = id_start + (id_max - id_min) + 1

        key = self._get_key(derived_type)
        self._statuses[key] = {'id_start': id_start    , 
                               'id_end'  : id_end      ,
                               'class'   : derived_type}


        







