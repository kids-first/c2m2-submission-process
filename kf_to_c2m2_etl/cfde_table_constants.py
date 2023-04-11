import os
import pandas as pd
import json



def get_table_cols_from_c2m2_json(table_name):
    json_path = os.path.join(os.getcwd(),'draft_C2M2_submission_TSVs','C2M2_datapackage.json')

    table_fields = []
    data = json.load(open(json_path,'r'))
    for resource in data.get('resources'):
        if resource.get('name') == table_name:
            for field in resource.get('schema').get('fields'):
                table_fields.append(field.get('name'))
    return table_fields 


def get_column_mappings(c2m2_entity_name: str):
    mapping_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl','conversion_tables','column_mapping.tsv')
    column_mapping_df = pd.read_table(mapping_path)
    column_mapping_df.query(f'c2m2_entity == "{c2m2_entity_name}"',inplace=True)
    mapping_dict = dict(zip(column_mapping_df.kf_col,column_mapping_df.c2m2_col))
    return mapping_dict

def get_hard_coded_columns(c2m2_entity_name: str):
    constants_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl','conversion_tables','table_constants.tsv')
    constants_df = pd.read_table(constants_path)
    constants_df.query(f'entity_name == "{c2m2_entity_name}"',inplace=True)
    mapping_dict = dict(zip(constants_df.col_name,constants_df.col_value))
    return mapping_dict

def add_constants(original_df: pd.DataFrame, c2m2_entity_name: str):
    for key, value in get_hard_coded_columns(c2m2_entity_name).items():
        original_df[key] = value

    return original_df.copy(deep=True)