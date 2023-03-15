import os
import pandas as pd


conversion_table_path = os.path.join(os.getcwd(),'data_ingestion','conversion_tables')
column_mapping_path = os.path.join(conversion_table_path,'column_mapping.tsv')


def is_tsv(file : os.DirEntry):
    return file.is_file() and file.name.endswith('.tsv')

def get_tables():
    with os.scandir(os.path.join(conversion_table_path)) as directory:
        return list(filter(is_tsv,directory))

def get_column_mapping(target_col: str):
    column_mapping_df = pd.read_table(column_mapping_path)
    result_df = column_mapping_df.query('kf_col == @target_col')
    return result_df.to_dict('records')[0]


def get_conversion_table(column: str):
    col_mapping = get_column_mapping(column) 

    for table in get_tables():
        if col_mapping['c2m2_col'] in table.name:
            return pd.read_table(table)
    

def kf_to_cfde_subject_value_converter(target_df: pd.DataFrame, target_column: str):
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