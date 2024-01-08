
import src.status as status
from collections import namedtuple
import pandas as pd
import os


class Status(status.Base):

    TBL_ONLY = status.Tuple(0, "Not matched to directory"       )
    DIR_ONLY = status.Tuple(1, "Not matched to table row"       )
    BOTH     = status.Tuple(2, "Matched directory and table row")
    
    def _is_valid(self): return self == Status.BOTH

Map = namedtuple("TableMap", "name col_name")

UPDATE_COLUMN='batch_update_count'
VALID_COLUMN="batch_is_row_still_valid"

class Table:

    def __init__(self, batch):
        
        self._df, self._df_idmap = batch._dstruc.to_dataframe()

        self._n_updates = 0
        #self._df[UPDATE_COLUMN] = self._n_updates 
        self._df_all = self._df.copy()
        self._logs_dpath = batch.dpaths.logs

    def filter(self, filter):

        raise NotImplementedError()



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


    def match_to_table_map(self, batch):

        ####################################################
        # TODO: Figure out suffix/job_id inclusion/sorting #
        ####################################################
        fpath, logger = batch._args.table_path, batch._logger 
        dmaps, tmaps = batch._dmaps, batch._tmaps
        logs_dpath = self._logs_dpath
        df_dir = self._df
        self._dtypes = dtypes = {m.name: m.dtype for m in dmaps}
        df_tbl = Table.read_table_map(fpath, logger, tmaps, dtypes)

        logger.banner("Matching Directories to Table") 
        df_both, df_only_dir, df_only_tbl = self._match_to_directories(df_tbl)

        if len(df_both) == 0: logger.error("No directories matched to table at path '%s'." % self._fpath)

        def init_table(df, status):
            df['status'] = status
            df[UPDATE_COLUMN] = self._n_updates
            df[VALID_COLUMN] = status.is_valid

            sort_columns = ([m.name for m in dmaps])
            sort_columns.append('status')
    
            # Put sorting columns at start of table
            sort_columns = [c for c in sort_columns if c in df.columns]
            for n in reversed(sort_columns):
                if n in df.columns:
                    df = df[[n] + [c for c in df.columns if not c == n]]

            return df.sort_values(by=sort_columns)




        df_both     = init_table(df_both    , Status.BOTH    )
        df_only_dir = init_table(df_only_dir, Status.DIR_ONLY)
        df_only_tbl = init_table(df_only_tbl, Status.TBL_ONLY)

        Table.log_missing(df_only_dir, logs_dpath, "orphan_directories.csv", "directories did not match to table entries", df_dir, logger)
        Table.log_missing(df_only_tbl, logs_dpath, "orphan_table_rows.csv" , "table entries did not match to directories", df_tbl, logger)


    @classmethod 
    def write_df(cls, df, fpath):

        df = df.copy()
        df['status'] = ["[%d] %s" % (n, s.display_string) for s, n in zip(df['status'], df[UPDATE_COLUMN])]
        df = df.drop(columns=[UPDATE_COLUMN, VALID_COLUMN])
        df.to_csv(fpath, index=False)

    @classmethod
    def log_missing(cls, df1, dpath, fname, msg, df2, logger):
        
        n1, n2 = len(df1), len(df2)
        if n1 == 0: return

        per=100.0*n1/n2
        fpath = os.path.join(dpath, fname)
        logger.warning("%d [%4.1f%%] %s, CSV file written to '%s'" % (n1, per, msg, fpath))     
        cls.write_df(df1, fpath)


    def _match_to_directories(self, df_tbl):

        df_dir = self._df

        # Columns ready for matching
        match_cols = [k for k, v in self._dtypes.items() if not v is float]

        # Number of decimals places for floats to be marked the same
        N_MATCH_DIGITS = 8
        # Creating temporary 'integerized' version of type float columns
        flt_cols = [k for k, v in self._dtypes.items() if v is float]
        tmp_cols = ["%s_TEMP_FLOAT_INT" % c for c in flt_cols]
        for c, tc in zip(flt_cols, tmp_cols):
            df_tbl[tc] = (df_tbl[c]*10**N_MATCH_DIGITS).astype(int)
            df_dir[tc] = (df_dir[c]*10**N_MATCH_DIGITS).astype(int)

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
        extra_drops = [c for c in df_dir.columns if c in df_only_tbl.columns]
        df_only_tbl = self.__clean_df(df_only_tbl, tmp_cols, '_dir_map', '_tbl_map', extra_drops)

        # Removing extra NaN columns gained from CSV/Table Dataframe
        extra_drops = [c for c in df_tbl.columns if c in df_only_dir.columns and c not in match_cols]
        df_only_dir = self.__clean_df(df_only_dir, tmp_cols, *suffixes, extra_drops)

        return df_both, df_only_dir, df_only_tbl

    def __clean_df(self, df, tmp_cols, drop_suffix, keep_suffix, extra_drops=[]):

        # Remove merge indicator column
        del_cols = ['_merge']
        # Removing duplicated columns from merge  
        print(del_cols); print('------------------------')
        del_cols.extend([c for c in df.columns if c.endswith(drop_suffix)])
        # Removing temporary 'integerized' float columns 
        print(del_cols); print('------------------------')
        del_cols.extend(tmp_cols)
        # Optional extra columns to remove 
        print(del_cols); print('------------------------')
        del_cols.extend(extra_drops)

        print(del_cols); print('=======================')
        df = df.drop(columns=del_cols)

        # Removing suffix from duplicate columns from merge 
        # Note: Replace command assumes suffix only appears at end of string
        col_map = {c: c.replace(keep_suffix,'') for c in df.columns if c.endswith(keep_suffix)}
        return df.rename(columns=col_map)

