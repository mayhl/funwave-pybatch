
import directorybatching.core.status as status

from collections import namedtuple
import pandas as pd
import os
import numpy as np

class Status(status.Base):

    TBL_ONLY = status.Tuple(0, "Not matched to directory"       )
    DIR_ONLY = status.Tuple(1, "Not matched to table row"       )
    BOTH     = status.Tuple(2, "Matched directory and table row")
    
    def _is_valid(self): return self == Status.BOTH

Map = namedtuple("TableMap", "name col_name")

STATUS_ID_COLUMN = 'batch_status_id'
VALID_COLUMN  = "batch_is_row_still_valid"
STATUS_COLUMN = "status"
LEAF_ID_COLUMN = "batch_dir_leaf_id"
FILES_COLUMN = 'job_files'

class Table:

    def __init__(self, batch):
        
        self._logs_dpath = batch.dpaths.logs
        self._dtypes = dtypes = {m.name: m.parser.dtype for m in batch._dmaps}
        self._smaps = batch._status_maps
        self._dmaps = batch._dmaps
        self._logger = batch.logger

        self._data_dpath = os.path.join(batch.dpaths.out, 'aggregate_data.csv')

        # Tree representation of directory structure 
        #self._dstruct = batch._dstruc
        # df       - Dataframe of directory parameters read 
        # df_idmap - Dictionary mapping some ID in dataframe to
        #            leafs nodes in order to propagate updates
        self._df, self._df_idmap = batch._dstruc.to_dataframe()
    
        # Dummy internal columns   
        df = self._df
        df[STATUS_ID_COLUMN] = df[STATUS_COLUMN].apply(self._smaps.get_id) 
        df[VALID_COLUMN] = True
        df[FILES_COLUMN] = None

        # Configuring dataframe column arrangement and row sorting 
        cols = ([m.name for m in batch._dmaps])
        self._sort_columns = cols.copy() ; self._sort_columns.append(STATUS_ID_COLUMN)
        self._start_columns = cols.copy(); self._start_columns.append(STATUS_COLUMN)
        self._df = self.__sort_df(self._df)
        

    def sync_data(self, updates={}, new_rtnvals={}):

        def update(row):

            is_valid = row[VALID_COLUMN]
            tp = type(is_valid)
            if tp is bool: return is_valid

            if issubclass(tp, status.Base): 
                return is_valid.is_valid

            raise TypeError()

        df = self._df
        df[VALID_COLUMN]  = df.apply(update, axis=1)
        Table.write_df(df, self._data_dpath)
        self._df = df

        if len(updates) > 0:
            for id, vals in updates.items():
                for name, val in vals.items():
                    setattr(self._df_idmap[id], name, val)
        else:
            for id, status in self._df[[LEAF_ID_COLUMN, STATUS_COLUMN]].values:
                self._df_idmap[id].status = status


        if len(new_rtnvals) > 0:
            for id, vals in new_rtnvals.items():
                self._df_idmap[id].rtnval.update(new_rtnvals)


    def prep_list_job_args(self):

        df = self._df[self._df[VALID_COLUMN]]

        # Hacky work around with int switch to float when 
        # NaN in column (missing data)
        leaf_ids = df[LEAF_ID_COLUMN].values.astype(np.longlong)

        # Removing internal columns 
        drop = [STATUS_ID_COLUMN, VALID_COLUMN, 
                LEAF_ID_COLUMN  , STATUS_COLUMN,
                FILES_COLUMN]
        df = df.drop(columns=drop)

        # Formatting args as a list 
        dpaths = [self._df_idmap[id].dpath for id in leaf_ids]
        records = df.to_dict('records')
        return list(zip(leaf_ids, dpaths, records))

    def update_from_jobs(self, jobs, results):


        appends = []
        updates = []
        stypes = []
        for j, r in zip(jobs, results):
            updates.append({LEAF_ID_COLUMN: j._leaf_id   , 
                            STATUS_COLUMN : r.status     ,
                            FILES_COLUMN  : j._files      })#,
                            #VALID_COLUMN  : r.is_continue})

            appends.append({**{LEAF_ID_COLUMN: j._leaf_id}, 
                            **r.job_params                })

            stypes.append(type(r.status))

        for stype in stypes:
            if not self._smaps.has(stype): self._smaps.append(stype)
        
        df = self._df
        
        df_append = pd.DataFrame.from_records(appends)
        if len(df_append.columns) > 1:
            df = df.merge(df_append, how='left', on=LEAF_ID_COLUMN, suffixes=['', '__JOB__'])


        df_update = pd.DataFrame.from_records(updates)

        ids = df_update[LEAF_ID_COLUMN].values
        cols = [c for c in df_update.columns if not c ==LEAF_ID_COLUMN]

        def update_status(row):
            if not row[LEAF_ID_COLUMN] in ids: return row[STATUS_COLUMN]
            other = df_update[df_update[LEAF_ID_COLUMN]==row[LEAF_ID_COLUMN]].iloc[0]
            return other[STATUS_COLUMN]

        for col in cols:

            def update_status(row):
                if not row[LEAF_ID_COLUMN] in ids: return row[col]
                other = df_update[df_update[LEAF_ID_COLUMN]==row[LEAF_ID_COLUMN]].iloc[0]
                return other[col]

            df[col] = df.apply(update_status, axis=1)

        def update_status_id(row):
            if not row[LEAF_ID_COLUMN] in ids: return row[STATUS_ID_COLUMN]
            return self._smaps.get_id(row[STATUS_COLUMN])
        

        #df[STATUS_COLUMN] = df.apply(update_status, axis=1)
        df[STATUS_ID_COLUMN] = df.apply(update_status_id, axis=1)


        appends = {i[LEAF_ID_COLUMN]: {k: v for k,v in i.items() if not k == LEAF_ID_COLUMN} for i in appends}
        updates = {i[LEAF_ID_COLUMN]: {k: v for k,v in i.items() if not k == LEAF_ID_COLUMN} for i in updates}

        #appends = {v: {k1: v1 for k1, v1 in appends if not k1==k} for k, v in appends.items() if k == LEAF_ID_COLUMN}
        #print(appends)
        self.sync_data(updates, appends)

        


    def __sort_df(self, df):

        is_sorted = np.all([a==b for a, b in zip(df.columns, self._start_columns)])

        if not is_sorted:
            for n in reversed(self._start_columns):
                if n in df.columns:
                    df = df[[n] + [c for c in df.columns if not c == n]]

        return df.sort_values(by=self._sort_columns)

    @classmethod
    def read_table_map(cls, fpath, logger, maps, dtypes):

        df = pd.read_csv(fpath)

        blank_col_names = [c for c in df.columns if c.startswith("Unnamed:")]
        if len(blank_col_names):
            logger.warning("Columns with no name have been dropped in CSV file '%s'." % fpath)
            df = df.drop(columns=blank_col_names)

        for map in maps:
            if not map.col_name in df.columns:
                logger.critical("Columnn '%s' does not exists in CSV table at '%s'." % (map.col_name, fpath)) 

        df = df.rename(columns={m.col_name: m.name for m in maps})

        return df.astype(dtypes)

    def sync_table_maps(self, batch):

        fpath = batch._args.table_path
        tmaps =  batch._tmaps
        logger = self._logger
        dtypes = self._dtypes
        dstruc = batch._dstruc

        self._smaps.append(Status)
        logger.banner("Matching Directories to Table") 
        df_tbl = Table.read_table_map(fpath, logger, tmaps, dtypes)
        df_only_tbl = self.__sync_and_extract_no_match(df_tbl)
        logger.info("Directories matched to table entries.")

        if len(df_only_tbl) > 0: 
            logger.info("Updating Directory Tree")
            count, new_id_map = dstruc.update_from_table(df_only_tbl)
            self._df_idmap.update(new_id_map)
            logger.warning("%d entries from table added to directory tree." % (count))    

        self._df = self.__sort_df(self._df)
        self.sync_data()


    def __sync_and_extract_no_match(self, df_tbl):
        # Updating chain statuses with Table Status
        logger = self._logger
        dmaps = self._dmaps
        logs_dpath = self._logs_dpath
        df_dir = self._df

        #df_tbl[LEAF_ID_COLUMN] = 0

        df_both, df_only_dir, df_only_tbl = self.__merge_split(df_tbl, dmaps)
        
        # Creating leaf ids for table only entries 
        max_id = max(np.max(df_both[LEAF_ID_COLUMN].values), np.max(df_only_dir[LEAF_ID_COLUMN].values))
        df_only_tbl[LEAF_ID_COLUMN] = np.arange(len(df_only_tbl)) + max_id + 1

        if len(df_both) == 0: logger.error("No directories matched to table at path '%s'." % self._table_fpath)

        def init_table(df, status):
            df[STATUS_ID_COLUMN] = self._smaps.get_id(status)
            df[STATUS_COLUMN] = status
            df[VALID_COLUMN] = status.is_valid
            df[LEAF_ID_COLUMN] = df[LEAF_ID_COLUMN].astype(int)
            # Merge resets to float
            return self.__sort_df(df)
            
        df_both     = init_table(df_both    , Status.BOTH    )
        df_only_dir = init_table(df_only_dir, Status.DIR_ONLY)
        df_only_tbl = init_table(df_only_tbl, Status.TBL_ONLY)

        Table.log_missing(df_only_dir, logs_dpath, "orphan_directories.csv", "directories did not match to table entries", df_dir, logger)
        Table.log_missing(df_only_tbl, logs_dpath, "orphan_table_rows.csv" , "table entries did not match to directories", df_tbl, logger)


        df = pd.concat([df_both, df_only_dir, df_only_tbl], ignore_index=True)
        self._df = df
        #cols = [LEAD_ID_COLUMN, STATUS_COLUMN]
        #updates = {i: s for i, s in df_both[cols].values)
        #updates.update({i: s for i,s in df_only_dir

        return df_only_tbl

    @classmethod 
    def write_df(cls, df, fpath):

        df = df.copy()

        #stat_info =[x for x in zip(df[STATUS_COLUMN], df[STATUS_ID_COLUMN])]
        #df[STATUS_COLUMN] = ["[%d] %s" % (n, s.display_string) for s, n in stat_info]

        def format_status(row):
            string = "[%d] %s" % (row[STATUS_ID_COLUMN], row[STATUS_COLUMN].display_string)
            if "msg" in row:
                msg = row["msg"]
                if not msg is None and not msg == "":
                    string = "%s %s" % (string, msg)

            return string

        df[STATUS_COLUMN] = df.apply(format_status, axis=1)
        
        df = df.drop(columns=[STATUS_ID_COLUMN, VALID_COLUMN])
        df.to_csv(fpath, index=False)

    @classmethod
    def log_missing(cls, df1, dpath, fname, msg, df2, logger):
        
        n1, n2 = len(df1), len(df2)
        if n1 == 0: return

        per=100.0*n1/n2
        fpath = os.path.join(dpath, fname)
        logger.warning("%d [%4.1f%%] %s, CSV file written to '%s'" % (n1, per, msg, fpath))     
        cls.write_df(df1, fpath)


    def __merge_split(self, df_tbl, dmaps):

        df_dir = self._df

        # Columns ready for matching
        match_cols = [d.name for d in dmaps if not d.parser.dtype is float]

        flt_cols = [(d.name, d.parser) for d in dmaps if d.parser.dtype is float]
        tmp_cols = ["%s__TEMP_COMPARE__" % n for n, _ in flt_cols]

        for (t, parser), tc in zip(flt_cols, tmp_cols):
            df_tbl[tc] = df_tbl[t].apply(parser.raw_reverse)
            df_dir[tc] = df_dir[t].apply(parser.raw_reverse)

        # Appending 'integerized' columns for merge
        match_cols.extend(tmp_cols)

        # Doing outer merge and using indicator to seperated matches as non-matches
        suffixes = ['_tbl_map', '_dir_map']
        total_merge = df_tbl.merge(df_dir, on=match_cols, how='outer', indicator=True, suffixes=suffixes)

        df_both = total_merge[total_merge['_merge']=='both']
        df_only_tbl = total_merge[total_merge['_merge']=='left_only']
        df_only_dir = total_merge[total_merge['_merge']=='right_only']

        # Cleaning up tables 
        df_both = self.__clean_df(df_both, tmp_cols, *suffixes)

        # Removing extra NaN columns gain for Directory Dataframe
        extra_drops = [c for c in df_dir.columns if c in df_only_tbl.columns and c not in match_cols]
        df_only_tbl = self.__clean_df(df_only_tbl, tmp_cols, *reversed(suffixes), extra_drops)

        # Removing extra NaN columns gained from CSV/Table Dataframe
        extra_drops = [c for c in df_tbl.columns if c in df_only_dir.columns and c not in match_cols]
        df_only_dir = self.__clean_df(df_only_dir, tmp_cols, *suffixes, extra_drops)

        return df_both, df_only_dir, df_only_tbl

    def __clean_df(self, df, tmp_cols, drop_suffix, keep_suffix, extra_drops=[]):

        # Remove merge indicator column
        del_cols = ['_merge']
        # Removing duplicated columns from merge  
        del_cols.extend([c for c in df.columns if c.endswith(drop_suffix)])
        # Removing temporary 'integerized' float columns 
        del_cols.extend(tmp_cols)
        # Optional extra columns to remove 
        del_cols.extend(extra_drops)

        df = df.drop(columns=del_cols)

        # Removing suffix from duplicate columns from merge 
        # Note: Replace command assumes suffix only appears at end of string
        col_map = {c: c.replace(keep_suffix,'') for c in df.columns if c.endswith(keep_suffix)}
        return df.rename(columns=col_map)

