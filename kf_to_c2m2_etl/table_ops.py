import pandas as pd

from cfde_table_constants import add_constants, get_column_mappings, get_table_cols_from_c2m2_json
from etl_types import ETLType


project_title_row = {'id_namespace':'kidsfirst:',
                                  'local_id':'drc',
                                  'persistent_id':'',
                                  'creation_time':'',
                                  'abbreviation':'KFDRC',
                                  'description':'''A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.''',
                                  'name':'The Gabriella Miller Kids First Pediatric Research Program'}

def apply_prefix_to_columns(the_df: pd.DataFrame):
    """
    Prefixes all columns in the DataFrame with the prefix of the primary key
    'kf_id', if it exists in the DataFrame.

    Args:
        the_df (pandas.DataFrame): DataFrame to prefix columns in.

    Returns:
        pandas.DataFrame: The modified DataFrame with columns prefixed, or the
        original DataFrame if the primary key was not present.
    """
    if ((row_index := the_df['kf_id'].first_valid_index()) is not None):
        prefix = the_df['kf_id'].loc[row_index].split('_')[0]
        the_df.rename(lambda col: f'{prefix}_{col}',axis='columns',inplace=True)
    return the_df


class TableJoiner:
    """
    Simplifies the act of joining kf entities. If the table contains a kf_id
    primary key, then before the tables are joined, all columns are prefixed
    with the prefix of the primary key in that table. This is only done for
    tables containing the kf_id primary key.
    The inspiration for this technique is derived from the sql's technique
    of applying aliases to resolve ambiguity for duplicate columns.
    """

    def __init__(self,base_table: pd.DataFrame=None):
        """
        Initializes a TableJoiner object.

        Args:
            base_table (pandas.DataFrame, optional): The base table to join
            other tables to. Defaults to None.
        """
        self.base_table=base_table


    def join_kf_table(self, join_table: pd.DataFrame, left_key, right_key=None):
        """
        Joins a KF table with the base table.

        Args:
            join_table (pandas.DataFrame): Table to join to the base table.
            left_key (str): Key in the base table to join on.
            right_key (str, optional): Key in the join table to join on.
            Defaults to None.

        Returns:
            TableJoiner: The TableJoiner object.
        """
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
        """
        Performs a left join of a table with the base table.

        Args:
            join_table (pandas.DataFrame): Table to join to the base table.
            left_key (str): Key in the base table to join on.
            right_key (str): Key in the join table to join on.

        Returns:
            TableJoiner: The TableJoiner object.
        """
        if 'kf_id' in self.base_table.columns:
            self.base_table = apply_prefix_to_columns(self.base_table)
        if 'kf_id' in join_table.columns:
            join_table = apply_prefix_to_columns(join_table)

        self.base_table = self.base_table.merge(join_table,
                                                how='left',
                                                left_on=left_key,
                                                right_on=right_key)

        return self


    def get_result(self):
        return self.base_table.copy(deep=True)


def reshape_kf_combined_to_c2m2(the_df: pd.DataFrame, entity_name):
    """
    Reshape a Kids First combined table into a C2M2-formatted table for a given entity.

    Args:
        the_df (pd.DataFrame): A Kids First combined table.
        entity_name (str): The name of the entity for which the table is being reshaped.

    Returns:
        pd.DataFrame: A C2M2-formatted table for the given entity.
    """
    the_df = add_constants(the_df, c2m2_entity_name=entity_name)

    the_df.rename(columns=get_column_mappings(ETLType.DS, entity_name),inplace=True)
    # Very disgusting
    if entity_name == 'file':
        the_df['size_in_bytes'].fillna(0,inplace=True)
        the_df['uncompressed_size_in_bytes'] = the_df['size_in_bytes']
        the_df = the_df.astype({"uncompressed_size_in_bytes":'int',
                                "size_in_bytes":'int'})

    if entity_name == 'project':
        the_df = pd.concat([pd.DataFrame(project_title_row,index=[0]),the_df]).reset_index(drop=True)

    the_df = the_df[get_table_cols_from_c2m2_json(entity_name)]

    return the_df

def is_column_present(file_path, column_name):
    try:
        # Read only the first row (header) of the CSV file
        header = pd.read_csv(file_path, nrows=1).columns.tolist()
        return column_name in header
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return False