import pandas as pd

from cfde_table_constants import add_constants, get_column_mappings, get_table_cols_from_c2m2_json

def remove_suffix(label: str):
    if label and label.endswith('_y'):
        return label.removesuffix('_y')
    else:
        return label

def remove_duplicate_columns(the_df: pd.DataFrame):
    the_df.drop([col for col in the_df.columns if '_x' in col],axis='columns',inplace=True)
    the_df.rename(remove_suffix,axis='columns',inplace=True)
    return the_df


class TableJoiner:

    def __init__(self,base_table: pd.DataFrame=None):
        self.base_table=base_table

    def join_table(self, join_table: pd.DataFrame, left_key, right_key=None):
        merge_kw_args = None
        if right_key:
            merge_kw_args = {'how':'inner','left_on':left_key,'right_on':right_key}
        else:
            merge_kw_args = {'how':'inner','on':left_key}

        self.base_table = self.base_table.merge(join_table,**merge_kw_args)

        self.base_table = remove_duplicate_columns(self.base_table)

        return self

    def left_join(self, join_table: pd.DataFrame, left_key, right_key):
        self.base_table = self.base_table.merge(join_table,
                                                how='left',
                                                left_on=left_key,
                                                right_on=right_key)
        if 'kf_id_x' in self.base_table.columns:
            self.update_primary_key(left_key,right_key)
        
        self.base_table = remove_duplicate_columns(self.base_table)

        return self

    def update_primary_key(self, left_key, right_key):
        old_primary = left_key if 'kf_id' not in left_key else right_key

        rename_dict = {}
        # if 'kf_id' in left_key:
        rename_dict['kf_id_x'], rename_dict['kf_id_y'] = old_primary, 'kf_id'
        # else:
        #     rename_dict['kf_id_x'], rename_dict['kf_id_y'] = 'kf_id', old_primary 

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
        the_df['uncompressed_size_in_bytes'] = the_df['size_in_bytes']

    the_df = the_df[get_table_cols_from_c2m2_json(entity_name)]

    return the_df