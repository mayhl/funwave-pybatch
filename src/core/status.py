

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

