import os
import sys
import numpy as np

def create_subdir(dpath, sub_dname):
    sub_dpath = os.path.join(dpath, sub_dname)
    if not os.path.exists(sub_dpath): os.mkdir(sub_dpath)
    return sub_dpath 

def get_calling_child_class(obj, mname):
    
    method = getattr(obj, mname)
    full_name = method.__qualname__
    *cls_parts, _ = full_name.split('.')
    return '.'.join(cls_parts)

def get_inheritance_diff(a, b):

    # NOTE: type return parent class if class is passed
    #print(a, b)
    #if not type(a) is type: a = type(a)
    #if not type(b) is type: b = type(b)

    #print(a, b)
    if a is b:
        return 0
    elif issubclass(a,b):
        return -1
    elif issubclass(b,a):
        return 1
    else:
        return None

class InheritanceRelationship:

    def __init__(self, cobj, bobj, sobj):

        self.__diff = get_inheritance_diff(sobj, bobj)

        # Checking that one and only one state is True
        smethods = np.array(['is_same', 'is_parent', 'is_child', 'is_diff'])
        states = np.array([getattr(self, m) for m in smethods])

        n_states = np.sum(states)
        if not n_states == 1:
            raise Exception("Invalid inheritiance relationship. Expected 1 state, got %d states: %s!" % (n_states, smethods[states]))


        if self.is_diff:
            self.__is_base = False
            self.__is_intermediary = False
            self.__is_self = False
        else:
            self.__is_base = get_inheritance_diff(cobj, bobj) == 0
            self.__is_self = get_inheritance_diff(cobj, sobj) == 0
            
            if self.is_base and self.is_self:
                raise Exception("Invalid inheritance relationship, calling node is both the base class and derivied class.")

            self.__is_intermediary = not self.is_base and not self.is_self

    @property
    def is_base(self): return self.__is_base

    @property
    def is_self(self): return self.__is_self

    @property
    def is_intermediary(self): return self.__is_intermediary

    @property
    def is_same(self): return self.__diff == 0

    @property 
    def is_parent(self): return self.__diff < 0

    @property
    def is_child(self): return self.__diff > 0

    @property
    def is_diff(self): return self.__diff is None

    @property
    def relation(self):
        if self.is_same  : return 'same'
        if self.is_parent: return 'parent'
        if self.is_child : return 'child'
        if self.is_diff  : return 'unrelated'

        raise Exception("Unexpected inheritance relationship string state.")

    @property
    def level(self):
        if self.is_base        : return 'base'
        if self.is_intermediary: return "intermediary"
        if self.is_self        : return "self"

        # Case for is_diff is True
        return None


def get_inheritance_relationship(a, b): return InheritanceRelationship(a,b)



def get_class_fullname(cls):
    return '%s.%s' % (cls.__module__, cls.__qualname__)


def parse_calling_child_class(obj, meth_name, base_obj):
    # Getting qualified class name components without method
    method = getattr(obj, meth_name)
    *comps, _ = get_class_fullname(method).split('.')

    # Getting object class
    calling_obj = sys.modules[comps[0]]
    for comp in comps[1:]: calling_obj = getattr(calling_obj, comp)

    # Reconstruct name with module attached
    name = '.'.join(comps)

    irel = InheritanceRelationship(calling_obj , base_obj, type(obj))

    return name, calling_obj, irel

