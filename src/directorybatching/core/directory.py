import directorybatching.core.status as status
import copy
import os
from anytree import Node, RenderTree, AsciiStyle, PostOrderIter
from abc import ABC, abstractmethod
from collections import namedtuple
import numpy as np
import logging
from termcolor import colored
from enum import Enum
import pandas as pd
from types import SimpleNamespace

Map = namedtuple("DirectoryMap", "name dir_name parser dtype")


class Status(status.Base):

    INVALID = status.Tuple(2, "Invalid"  )
    PARTIAL = status.Tuple(1, "Partially")
    VALID   = status.Tuple(0, "Valid"    )

    def _is_valid(self): return self == Status.VALID

def test_parser(parser, name, value, is_last=False, is_print=True):
    is_valid, msg = _test_parser(parser, name, value, is_last)
    if is_print: print(msg)
    return is_valid, msg

def _test_parser(func, name, value, is_last):

    try:
        rtn_dict = func(name, value)
    except Exception as e:
        return False, "[ERROR] Input: %s | Parser threw exception!"  % value
    
    if not type(rtn_val) is dict:
        return False, "[ERROR] Parser did not return a dict!"

    for key in 'value':
        if key not in rtn_dict:
            return False, "[ERROR] Dict does not have the required key '%s'." % key

    if rtn_dict['value'] is None:
        return True, "[IGNORE] Input: %s | Parser returned None so directory will be ignored." % value

    if 'suffix' in rtn_dict and not is_last:
        return False, "[ERROR] Dict has key 'suffix' but you specified, is_last=True, that parser was not for last nested directory."

    return True, "[SUCCESS]: Input: %s | Output: %s " % (value, rtn_dict)


class Structure:

    def __init__(self, batch):

        self._dpaths = SimpleNamespace(
                out  = batch.dpaths.out ,
                logs = batch.dpaths.logs)

        self._logger = batch.logger

        maps = batch._dmaps
        root_dpath = batch.dpaths.root

        self._root = Node(root_dpath, is_valid=Status.VALID, dpath=root_dpath, is_vleaf=False)
        self._n_updates = 0
        self.__build_tree(root_dpath, maps)
        self.__check_tree_depth(maps)

        self._dtypes = {m.name: m.dtype for m in maps}

    @property
    def dpaths(self): return self._dpaths


    @property
    def leafs(self):
        return [n for n in PostOrderIter(self._root, filter_=lambda n: n.is_leaf)]

    @property
    def vleafs(self):
        return [n for n in PostOrderIter(self._root, filter_=lambda n: n.is_vleaf)]

    def to_dataframe(self):

        leafs = self.leafs
        data = [self.__leaf_to_dict(l, id) for id, l in enumerate(leafs)]
        map = {r['dir_id']: l for r, l in zip(data,leafs)}

        df = pd.DataFrame.from_records(data).astype(self._dtypes)

        return df, map

    def __leaf_to_dict(self, l, map_id):

        total = {'dir_id': map_id} 
        while not l.is_root:
            total.update({k: v for k, v in l.rtnval.items() if not k =='value'})
            total[l.bname] = l.rtnval['value'] 
            l = l.parent

        return total

    def _update_status(self, p=None):
        if p is None: 
            p = self._root
            self._n_updates += 1

        if not p.is_leaf:
            statuses =  np.array([self._update_status(c) for c in p.children])

            if Status.all_valid(statuses):
                p.status = Status.VALID
            elif Status.any_valid(statuses):
                p.status = Status.INVALID
            else:
                p.status = Status.PARTIAL

        elif p.is_vleaf:
            statuses =  np.array([self._update_status(c) for c in p.children])
            p._status = Status.min(statuses)

        return p.status 

    def print_tree_status(self, name=None):

        marks = {Status.VALID  : colored('✓', 'green' ),
                 Status.INVALID: colored('x', 'red'   ),
                 Status.PARTIAL: colored('-', 'yellow')}

        log_fname = "filter_stage_%02d" % self._n_updates
        if not name is None: log_fname += "_%s" % name
        log_fname += ".text"
        log_fpath = os.path.join(self.dpaths.logs, log_fname)

        with open(log_fpath, 'w') as f:
            for pre, _, node in RenderTree(self._root):
                mark = marks[node.status]
                f.write("%s [%s] %s\n" % (pre, mark, node.name))

    def filter_valid(self, name=None):

        self._update_status()
        self.print_tree_status(name)

        leafs = self.leafs
        
        nt = len(leafs)
        are_valid = [l.status.is_valid for l in leafs]
        nv = np.sum(are_valid)
        nr = nt - nv

        self._logger.info("Removing %d out of %d leafs in filter stage %d '%s'."  % (nr, nt, self._n_updates, name))

        for l, is_valid in zip(leafs, are_valid):
            if not is_valid: self.remove_leaf(l)


    def remove_leaf(self, l):
        logger = self._logger
        if not l.is_leaf:
            logger.critical("Can not remove leaf as it is not a leaf, '%s%'" % l.dpath)

        p = l.parent

        nb = len(p.children)
        p.children = (c for c in p.children if not c==l)
        na = len(p.children)

        if nb - na > 1:
            logger.critical("Removed more than one node in method 'remove_leaf' when removing '%s'!!!" % l.dpath)

        if nb == na:
            logger.debug("Did not find leaf node '%s'." % l.dpath)
        else:
            logger.debug("Removing leaf node '%s.'" % l.dpath)

        if na == 0:
            logger.debug("Removed parent node '%s' since it has no children after removing child '%s'." % (p.dpath, l.name)) 
            self.remove_leaf(p)

    def __process_subdir(self, sub_dname, dpath, map, is_last):

        def qreturn(is_valid, rtnvalue={}, is_vleaf=False):

            status = Status.VALID if is_valid else Status.INVALID
            attrs = {'dpath'   : sub_dpath   ,
                     "status"  : status      ,
                     "bname"   : map.dir_name,
                     "is_vleaf": is_vleaf    ,
                     'rtnval'  : rtnval       }

            return is_vleaf, (sub_dname, attrs)

        logger = self._logger
        sub_dpath = os.path.join(dpath, sub_dname)

        try:
            rtnval = map.parser(map.dir_name, sub_dname)
        except Exception as e:
            logger.config("Subfolder '%s' caused an error in the parser for directory map '%s' "\
                          "at path '%s'." % (sub_dname, map.dir_name, dpath)                     )

        if rtnval is None: 
            logger.warning("Subfolder '%s' did not parse for directory map '%s' at "\
                           "path '%s'." % (sub_dname, map.dir_name, dpath)           )
            return qreturn(False)
       
        if not type(rtnval) is dict:
            logger.config("Parser for directory map, '%s', did not return a dict: got "\
                          "type '%s'." % (map.dir_name, type(rtn_val)))
    
        # FUTURE IDEA: Remove value keyword restriction
        if not 'value' in rtnval:
            logger.config("Parser for directory map, '%s', did not return dict with: "\
                          "keyword 'value'." % (map.dir_name)                          )

        is_vname = 'virtual_name' in rtnval
        is_vval  = 'virtual_value' in rtnval
        is_vleaf = is_vname and is_vval

        if not is_vname == is_vval:
            logger.config("Parser for directory map, '%s', return a dict with incomplete "\
                          "virtual directory keywords." % (map.dir_name))

        if is_last and not is_vleaf:
            logger.config("Parser for last directory map, '%s', did not return a virtual "\
                          "directory keywords." % (map.dir_name))
        
        if not is_last and is_vleaf: 
            logger.config("Parser for directory map, '%s', returned a dictionary with virtual "\
                          "directory keywords when it should not have." % (map.dir_name))

        return qreturn(True, rtnval, is_vleaf)

    def __build_tree(self, dpath, maps, parent=None):

        # End of recursive calling
        nmaps = len(maps)
        if nmaps == 0: return None

        # Initial recursive call initialization
        is_root = parent is None
        if is_root: parent = self._root

        # Parsing subdirectory names 
        is_last = nmaps == 1
        out_dname = os.path.basename(self._dpaths.out)
        subinfo = [self.__process_subdir(f.name, dpath, maps[0], is_last) for f in os.scandir(dpath) if f.is_dir() and not f.name == out_dname]
        is_vleaf_list, subinfo = zip(*subinfo)

        # Safety check 
        if np.all(is_vleaf_list):
            has_vleafs = True
        elif np.all(np.invert(is_vleaf_list)):
            has_vleafs = False
        else:
            self._logger.critical("Only some of node's children reported having a suffix.")
        
        if not has_vleafs:

            # Recursive calls to build child nodes 
            children = [Node(name, parent, **attrs) for name, attrs in subinfo]
            for child in children: self.__build_tree(os.path.join(dpath,child.name), maps[1:], child)

        else:
            # Special case for final descendant which has a suffix

            # Collecting children for each parent and cleaning up attributes
            subnodes = {}
            for name, attrs in subinfo:
                # Special case for first parent
                if not name in subnodes: 
                    pattrs = copy.deepcopy(attrs)
                    pattrs['dpath'] = None
                    del pattrs['rtnval']['virtual_name']
                    del pattrs['rtnval']['virtual_value']

                    subnodes[name] = {'attrs': pattrs, 'children': []}
                
                attrs['is_vleaf'] = False
                attrs['bname'] = attrs['rtnval']['virtual_name']
                attrs['value'] = attrs['rtnval']['virtual_value']
                vname = attrs['rtnval']['virtual_name']
                del attrs['rtnval']['virtual_name']
                del attrs['rtnval']['virtual_value']

                subnodes[name]['children'].append((vname, attrs))

            # Creating children and grandchildren
            for cname in subnodes:
                child = Node(cname, parent, **subnodes[cname]['attrs'])
                for name, attrs in subnodes[cname]['children']: Node(name, child, **attrs)

        if is_root:
            self.filter_valid("dir_mapping")

    def __check_tree_depth(self, maps):

        logger = self._logger

        # Checking depth of tree match directory map depth
        max_depth_tree = max([l.depth for l in self.leafs])

        # Hacky check 
        self._has_vleafs = len(self.vleafs) > 0

        # Ignoring last tree depth for virtual leafs 
        if self._has_vleafs: max_depth_tree -= 1
        max_depth_map = len(maps)
        if max_depth_tree > max_depth_map:
            logger.critical("Max tree depth greater than number of directory maps: specified %d, found %d!!!" % (max_depth_map, max_depth_tree))

        if max_depth_tree < max_depth_map:
            logger.error("No directories exist at specfied directory map depth: specified %d, found %d!" % (max_depth_map, max_depth_tree))

        max_depth = max_depth_map
        if self._has_vleafs: max_depth += 1
        for l in self.leafs: l.status = Status.VALID if l.depth == max_depth else Status.INVALID

        self.filter_valid('depth_check')

    def __flatten_vleafs(self):

        vleafs = self.vleafs
        for vl in self.vleafs:
            
            min_status = min([c.status for c in vl.children])

            ### NOT ALWAYS THE CASE ##################
            # Assume last sorted child value is best #
            ##########################################

            best_cname =sorted([c.name for c in vl.children if c.status == min_status])[-1]  

            # Sync attributes 
            for c in vl.children:

                if not c.name == best_cname: continue

                # Hacky but independent of parser 
                vl.name = os.path.basename(c.dpath)
                vl.dpath = c.dpath
                vl.is_vleaf = False
                # vl.suffixes = None
                del vl.children

                break

