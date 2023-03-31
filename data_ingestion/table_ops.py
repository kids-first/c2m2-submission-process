import pandas as pd
from collections import defaultdict
from inspect import signature

from cfde_table_constants import add_constants, get_column_mappings, get_table_cols_from_c2m2_json


project_title_row = {'id_namespace':'kidsfirst:',
                                  'local_id':'drc',
                                  'persistent_id':'',
                                  'creation_time':'',
                                  'abbreviation':'KFDRC',
                                  'description':'''A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.''',
                                  'name':'The Gabriella Miller Kids First Pediatric Research Program'}

def apply_prefix_to_columns(the_df: pd.DataFrame):
    if ((row_index := the_df['kf_id'].first_valid_index()) is not None):
        prefix = the_df['kf_id'].loc[row_index].split('_')[0]
        the_df.rename(lambda col: f'{prefix}_{col}',axis='columns',inplace=True)
    return the_df


def remove_suffix_right_table(label: str):
    if label and label.endswith('_y'):
        return label.removesuffix('_y')
    else:
        return label

def replace_suffix_with_alias(label: str, alias: str, suffix: str):
    if label and label.endswith(suffix):
        return f'{alias}_' + label.removesuffix(suffix)
    else:
        return label


def remove_duplicate_columns(the_df: pd.DataFrame):
    """
    This method is intended to remove duplicate columns resulting from joining
    kf entities. At one point, updating the primary key as a table was joined
    and attempting to remove the duplicate led to the loss of data in left joins
    in particular. Should continue future proofing more when time permits.
    """
    the_df.drop([col for col in the_df.columns if '_x' in col],axis='columns',inplace=True)
    the_df.rename(remove_suffix_right_table,axis='columns',inplace=True)
    return the_df


class TableJoiner:
    """
    Simplifies the act of joining kf entities. Handles duplicate columns
    and updating the names of primary keys. Keeps the primary key of the
    table joined from the right and renames primary key of the left table 
    to retain that column. For the remaining columns, it simply drops 
    the duplicates of the table joined on the left.
    """

    def __init__(self,base_table: pd.DataFrame=None):
        self.base_table=base_table


    def join_kf_table(self, join_table: pd.DataFrame, left_key, right_key=None):
        merge_kw_args = None
        if right_key:
            merge_kw_args = {'how':'inner','left_on':left_key,'right_on':right_key}
        else:
            merge_kw_args = {'how':'inner','on':left_key}

        if 'kf_id' in self.base_table.columns:
            self.base_table = apply_prefix_to_columns(self.base_table)
        if 'kf_id' in join_table.columns:
            join_table = apply_prefix_to_columns(join_table)

        self.base_table = self.base_table.merge(join_table,**merge_kw_args)

        return self

    def left_join(self, join_table: pd.DataFrame, left_key, right_key):
        
        if 'kf_id' in self.base_table.columns:
            self.base_table = apply_prefix_to_columns(self.base_table)
        if 'kf_id' in join_table.columns:
            join_table = apply_prefix_to_columns(join_table)

        self.base_table = self.base_table.merge(join_table,
                                                how='left',
                                                left_on=left_key,
                                                right_on=right_key)

        return self

    def update_primary_key(self, left_key, right_key):
        old_primary = left_key if 'kf_id' not in left_key else right_key

        rename_dict = {}
        rename_dict['kf_id_x'], rename_dict['kf_id_y'] = old_primary, 'kf_id'

        self.base_table.rename(columns=rename_dict,inplace=True)

        drop_cols = [col for col in self.base_table.columns 
                     if (isinstance(col,str) and 
                         (col.endswith('_x') or col.endswith('_y')) and
                          col.startswith(old_primary))]

        self.base_table.drop(drop_cols,axis='columns',inplace=True)


    def get_result(self):
        return self.base_table.copy(deep=True)


def reshape_kf_combined_to_c2m2(the_df: pd.DataFrame, entity_name):
    the_df = add_constants(the_df, c2m2_entity_name=entity_name)

    the_df.rename(columns=get_column_mappings(entity_name),inplace=True)
    # Very disgusting
    if entity_name == 'file':
        the_df['size_in_bytes'].fillna(0,inplace=True)
        the_df['uncompressed_size_in_bytes'] = the_df['size_in_bytes']
        the_df = the_df.astype({"uncompressed_size_in_bytes":'int',
                                "size_in_bytes":'int'})

    the_df = the_df[get_table_cols_from_c2m2_json(entity_name)]

    return the_df