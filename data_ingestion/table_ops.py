import pandas as pd

from cfde_table_constants import add_constants, get_column_mappings, get_table_cols_from_c2m2_json

def udpate_primary_key(the_df: pd.DataFrame):
    the_df.rename({'kf_id_y':'kf_id'},axis='columns',inplace=True)
    the_df.drop(['kf_id_x'],axis='columns',inplace=True)
    return the_df

def remove_suffix(label: str):
    if label and label.endswith('_y'):
        return label.removesuffix('_y')
    else:
        return label

def remove_duplicate_columns(the_df: pd.DataFrame):
    if 'kf_id_x' in the_df.columns:
        the_df = udpate_primary_key(the_df)

    the_df.drop([col for col in the_df.columns if '_x' in col],axis='columns',inplace=True)
    the_df.rename(remove_suffix,axis='columns',inplace=True)
    return the_df


class TableJoiner:

    def __init__(self,base_table: pd.DataFrame=None):
        self.base_table=base_table

    def join_table(self, join_table: pd.DataFrame, left_key, right_key=None, join_type=None):

        if not join_type:
            join_type = 'inner'

        merge_kw_args = None
        if right_key:
            merge_kw_args = {'how':join_type,'left_on':left_key,'right_on':right_key}
        else:
            merge_kw_args = {'how':join_type,'on':left_key}

        self.base_table = self.base_table.merge(join_table,**merge_kw_args)

        self.base_table = remove_duplicate_columns(self.base_table)

        return self

    def left_join(self, join_table: pd.DataFrame, left_key, right_key=None):
        self.join_table(join_table,left_key,right_key,'left')
        return self

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