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

Map = namedtuple("DirectoryMap", "name dir_name parser")


LEAF_ID_COLUMN = 'batch_dir_leaf_id'
STATUS_COLUMN  = 'status'

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
        self._smaps = batch._status_maps
        self._smaps.append(Status)

        self._dmaps = dmaps = batch._dmaps
        root_dpath = batch.dpaths.root

        self._root = Node(root_dpath, is_valid=Status.VALID, dpath=root_dpath, is_vleaf=False)
        self._n_updates = 0
        self.__build_tree(root_dpath, dmaps)
        self.__check_tree_depth(dmaps)

        self._dtypes = {m.name: m.parser.dtype for m in dmaps}

    @property
    def dpaths(self): return self._dpaths

    @property
    def leafs(self):
        return [n for n in PostOrderIter(self._root, filter_=lambda n: n.is_leaf)]

    @property
    def vleafs(self):
        return [n for n in PostOrderIter(self._root, filter_=lambda n: n.is_vleaf)]

    ###########################
    # Directory/Table Methods #
    ###########################

    def to_dataframe(self, ids=None):

        if not ids is None: raise NotImplementedError()
   
        leafs = self.leafs
        if ids is None: ids = list(range(len(leafs)))

        data = [self.__leaf_to_dict(l, id) for id, l in zip(ids, leafs)]
        map = {r[LEAF_ID_COLUMN]: l for r, l in zip(data,leafs)}
        
        df = pd.DataFrame.from_records(data).astype(self._dtypes)
        df[LEAF_ID_COLUMN] = df[LEAF_ID_COLUMN].astype(int)
        return df, map

    def __leaf_to_dict(self, l, id):

        total = {LEAF_ID_COLUMN: id, 
                 STATUS_COLUMN : l.status} 
        while not l.is_root:
            total.update(l.rtnval)
            l = l.parent

        return total

    def update_from_table(self, df):

        COMPARE_TAG = "%s__TEMP_COMPARE__"
        info =[]
        for m in self._dmaps:
            p = m.parser
            if p.dtype is float:
                compare_col = COMPARE_TAG % m.name
                df[compare_col]=df[m.name].apply(p.raw_reverse)
            else:
                compare_col = m.name
            info.append((m, compare_col))
            
        count = self._update_from_table(self._root, df, info)
        return count 
    def _update_from_table(self, p, df, info):

        if len(info) == 0: return 0 

        dmap, comp_col = info[0]
        logger = self._logger

        if np.any([not c.bname==dmap.dir_name for c in p.children]):
            raise logger.critical("Directory maps order didn't match "\
                                  "tree structure.", TypeError)

        t_vals = np.unique(df[comp_col].values)
        d_vals = [c.rtnval[dmap.name] for c in p.children]

        
        is_float = dmap.parser.dtype is float
        if is_float:
            reverse = dmap.parser.raw_reverse
            d_vals = [reverse(v) for v in d_vals]

        new_vals = [v for v in t_vals if not v in d_vals]


        is_last = len(info) == 1
        if len(new_vals) > 0:
            for val in new_vals:
                self.__add_table_node(p, val, df, dmap, is_float, is_last)

        n_count = 0
        for c in p.children:
            val = c.rtnval[c.bname]
            if is_float: val = reverse(val)
            sub_df = df[df[comp_col]==val]
            if len(sub_df) == 0: continue
            n_count += self._update_from_table(c, sub_df, info[1:])

        return len(new_vals) if is_last else n_count

    def __add_table_node(self, p, val, df, dmap, is_float, is_last):

        logger = self._logger 
        if is_float:
            forward = dmap.parser.raw_forward
            val = forward(val)
        
        has_preproc = dmap.parser.has_preprocessor
        rtnval = {dmap.name: val}
        name = dmap.parser.reverse(dmap.name, rtnval, has_preproc)


        is_vleaf = is_last and has_preproc
        status = df.iloc[0][STATUS_COLUMN] if is_last and not is_vleaf else None

        args = (False, None, status, dmap.name, is_vleaf, rtnval)
        attrs = Structure.__gen_node_attrs(*args)

        c = Node(name, p, **attrs)
        logger.debug("Added node %s" % c)

        if is_vleaf:
            status = df.iloc[0][STATUS_COLUMN]
            is_vleaf = False 
            name = "__DUMMY__"
            rtnval = {'jobid': "__DUMMY__"} 
            
            args = (False, None, status, name, is_vleaf, rtnval)
            attrs = Structure.__gen_node_attrs(*args)
            gc = Node(name, c, **attrs)
            logger.debug("Added virtual node %s" % gc)




    ####################
    # Internal methods #
    ####################

    def _update_status(self, p=None):
        if p is None: 
            p = self._root
            self._n_updates += 1

        if not p.is_leaf or p.is_vleaf:

            ids, is_valids = zip(*[self._update_status(c) for c in p.children])

            self._is_valid = is_valid = Status.max(is_valids)

            id_max = np.max(ids)
            id_min = np.min(ids)

            p._status_max = self._smaps.get_status(id_max)
            p._status_min = self._smaps.get_status(id_min)

            p.status = p._status_max
            id = id_max

        else:

            id = self._smaps.get_id(p.status)
            is_valid   = p.is_valid
          
            if type(p.is_valid) is bool:
                is_valid = Status.VALID if is_valid else Status.INVALID
                p.is_valid = is_valid

        return id, is_valid

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
                mark = marks[node.is_valid] 
                f.write("%s [%s] %s\n" % (pre, mark, node.name))

    def filter_valid(self, name=None):

        self._update_status()
        self.print_tree_status(name)

        leafs = self.leafs
        
        nt = len(leafs)
        are_valid = [l.is_valid.is_valid for l in leafs]
        nv = np.sum(are_valid)
        nr = nt - nv

        self._logger.info("Removing %d out of %d leafs in filter stage %d '%s'."  % (nr, nt, self._n_updates, name))

        #for l, is_valid in zip(leafs, are_valid):
         #   if not is_valid: self.remove_leaf(l)


    def __remove_leaf(self, l):
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

    @classmethod
    def __gen_node_attrs(cls, is_valid, dpath, status, bname,
                       is_vleaf=False, rtnval={}):

        status = Status.VALID if is_valid else Status.INVALID
        attrs = {'dpath'   : dpath   ,
                 "status"  : status  ,
                 "bname"   : bname   ,
                 "is_valid": status  ,
                 "is_vleaf": is_vleaf,
                 'rtnval'  : rtnval  }

        return attrs

    def __process_subdir(self, sub_dname, dpath, map, is_last):

        def qreturn(is_valid, name=sub_dname, rtnvalue={}, is_vleaf=False): 
            args = (is_valid, sub_dpath, status, map.dir_name, is_vleaf, rtnval)
            attrs = Structure.__gen_node_attrs(*args)
            return is_vleaf, (name, attrs)

        logger = self._logger
        sub_dpath = os.path.join(dpath, sub_dname)

        try:
            rtnval = map.parser.forward(map.dir_name, sub_dname)
        except Exception as e:
            raise e
            logger.config("Subfolder '%s' caused an error in the parser for directory map '%s' "\
                          "at path '%s'." % (sub_dname, map.dir_name, dpath)                     )

        if rtnval is None: 
            logger.warning("Subfolder '%s' did not parse for directory map '%s' at "\
                           "path '%s'." % (sub_dname, map.dir_name, dpath)           )
            return qreturn(False)
       
        if not type(rtnval) is dict:
            logger.config("Parser for directory map, '%s', did not return a dict: got "\
                          "type '%s'." % (map.dir_name, type(rtn_val)))
    
        if not map.dir_name in rtnval:
            logger.config("Parser for directory map, '%s', did not return dict with: "\
                          "corresponding keyword '%s'." % (map.dir_name, map.dir_name)              )

        is_vleaf = 'virtual' in rtnval

        if is_last and not is_vleaf:
            logger.config("Parser for last directory map, '%s', did not return a virtual "\
                          "directory keywords." % (map.dir_name))
        
        if not is_last and is_vleaf: 
            logger.config("Parser for directory map, '%s', returned a dictionary with virtual "\
                          "directory keywords when it should not have." % (map.dir_name))


        name = map.parser.reverse(map.dir_name, rtnval, True) if is_vleaf else sub_dname 
        return qreturn(True, name, rtnval, is_vleaf)

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
                    del pattrs['rtnval']['virtual']

                    subnodes[name] = {'attrs': pattrs, 'children': []}
                
                attrs['is_vleaf'] = False

                vattr = attrs['rtnval']['virtual']
                if not len(vattr) == 1: raise Exception()
                vname, vvalue = list(vattr.items())[0]
                attrs['bname'] = vname
                attrs['value'] = vvalue
                del attrs['rtnval']['virtual']
                
                subnodes[name]['children'].append((vvalue, attrs))

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

