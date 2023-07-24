import os
import pandas as pd
from collections import OrderedDict
from file_locations import file_locations


column_mapping_path = os.path.join(file_locations.get_kf_to_c2m2_mappings_path(),'column_mapping.tsv')

def is_tsv(file : os.DirEntry):
    """
    Determines whether a given file is a TSV file.

    Args:
        file (os.DirEntry): A file object.

    Returns:
        bool: True if the file is a TSV file, False otherwise.
    """
    return file.is_file() and file.name.endswith('.tsv')

def get_tables():
    """
    Gets a list of TSV files from the conversion table directory.

    Returns:
        list: A list of file objects.
    """
    table_paths = []

    table_directories = [file_locations.get_cfde_reference_table_path(),file_locations.get_ontology_mappings_path()]
    for file_path in table_directories: 
        with os.scandir(file_path) as directory:
            table_paths += list(filter(is_tsv,directory))
    
    return table_paths

def get_column_mapping(target_col: str):
    """
    Gets a dictionary of column mappings for a given target column.

    Args:
        target_col (str): The target column to map.

    Returns:
        dict: A dictionary containing the column mappings.
    """
    column_mapping_df = pd.read_table(column_mapping_path)
    result_df = column_mapping_df.query('kf_col == @target_col')
    return result_df.to_dict('records')[0]


def get_conversion_table(column: str):
    """
    Gets the conversion table for a given column.

    Args:
        column (str): The target column to convert.

    Returns:
        pd.DataFrame: A dataframe containing the conversion table.
    """
    col_mapping = get_column_mapping(column) 

    for table in get_tables():
        if col_mapping['c2m2_col'] in table.name:
            return pd.read_table(table)
    

def kf_to_cfde_value_converter(target_df: pd.DataFrame, target_column: str):
    """
    Converts a target column in a given dataframe to the corresponding CFDE value.

    Args:
        target_df (pd.DataFrame): A pandas dataframe containing the target column.
        target_column (str): The target column to convert.

    Returns:
        pd.DataFrame: The modified dataframe with the converted column.
    """
    col_mapping = get_column_mapping(target_column)

    conversion_table = get_conversion_table(target_column) 
    target_column_df = target_df[[target_column]]

    conversion_df = target_column_df.merge(conversion_table,
                                        how='left',
                                        left_on=target_column,
                                        right_on='name'
                                        )[['id']]
    
    conversion_df.rename(columns={'id':col_mapping['c2m2_col']},inplace=True)

    target_df[col_mapping['kf_col']] = conversion_df[col_mapping['c2m2_col']]

    return target_df

uberon_mapping_df = pd.read_csv(os.path.join(file_locations.get_ontology_mappings_path(),'anatomy_fixed_tabs.tsv'),sep='\t').fillna('')
uberon_mapping_df['composition_term'] = uberon_mapping_df['composition_term'].apply(str.lower)
uberon_mapping_df['uberon_id'] = uberon_mapping_df['uberon_id'].apply(str.lower)
uberon_mapping_df = uberon_mapping_df[::-1]
uberon_mapping_dict = OrderedDict(zip(uberon_mapping_df.composition_term,uberon_mapping_df.uberon_id))