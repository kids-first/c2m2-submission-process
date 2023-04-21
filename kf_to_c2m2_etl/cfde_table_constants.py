import os
import pandas as pd
import json



def get_table_cols_from_c2m2_json(table_name):
    """
    Get column names from the given C2M2 JSON file for the specified table.

    Args:
        table_name (str): The name of the table to get the columns for.

    Returns:
        List[str]: A list of column names for the specified table.
    """
    json_path = os.path.join(os.getcwd(),'draft_C2M2_submission_TSVs','C2M2_datapackage.json')

    table_fields = []
    data = json.load(open(json_path,'r'))
    for resource in data.get('resources'):
        if resource.get('name') == table_name:
            for field in resource.get('schema').get('fields'):
                table_fields.append(field.get('name'))
    return table_fields 


def get_column_mappings(c2m2_entity_name: str):
    """
    Get a dictionary of column mappings from KF to C2M2 for the specified entity.

    Args:
        c2m2_entity_name (str): The name of the C2M2 entity to get the column mappings for.

    Returns:
        dict: A dictionary of column mappings from KF to C2M2 for the specified entity.
    """
    mapping_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl','conversion_tables','column_mapping.tsv')
    column_mapping_df = pd.read_table(mapping_path)
    column_mapping_df.query(f'c2m2_entity == "{c2m2_entity_name}"',inplace=True)
    mapping_dict = dict(zip(column_mapping_df.kf_col,column_mapping_df.c2m2_col))
    return mapping_dict

def get_hard_coded_columns(c2m2_entity_name: str):
    """
    Get a dictionary of hard-coded constants for the specified C2M2 entity.

    Args:
        c2m2_entity_name (str): The name of the C2M2 entity to get the constants for.

    Returns:
        dict: A dictionary of hard-coded constants for the specified C2M2 entity.
    """
    constants_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl','conversion_tables','table_constants.tsv')
    constants_df = pd.read_table(constants_path)
    constants_df.query(f'entity_name == "{c2m2_entity_name}"',inplace=True)
    mapping_dict = dict(zip(constants_df.col_name,constants_df.col_value))
    return mapping_dict

def add_constants(original_df: pd.DataFrame, c2m2_entity_name: str):
    """
    Add hard-coded constants to the specified dataframe for the specified C2M2 entity.

    Args:
        original_df (pd.DataFrame): The original dataframe to add constants to.
        c2m2_entity_name (str): The name of the C2M2 entity to add constants for.

    Returns:
        pd.DataFrame: A copy of the original dataframe with the hard-coded constants added.
    """
    for key, value in get_hard_coded_columns(c2m2_entity_name).items():
        original_df[key] = value

    return original_df.copy(deep=True)