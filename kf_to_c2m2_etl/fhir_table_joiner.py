import os

import pandas as pd

from file_locations import file_locations
from table_ops import add_constants, get_column_mappings
from cfde_table_constants import get_table_cols_from_c2m2_json
from etl_types import ETLType

fhir_resource_types = ['ResearchStudy','Patient','Specimen','DocumentReference']

class FhirDataJoiner:
    foreign_key_mappings = {
        'Patient': {'Specimen': {'left': 'Patient_id', 'right': 'Specimen_subject_reference'},
                    'DocumentReference': {'left': 'Patient_id', 'right': 'DocumentReference_subject_reference'}},
        'Specimen': {'Patient': {'left': 'Specimen_subject_reference', 'right': 'Patient_id'},
                     'DocumentReference': {'left': 'Specimen_id', 'right': 'DocumentReference_context_related_0_reference'}},
        'DocumentReference': {'Patient': {'left': 'DocumentReference_subject_reference', 'right': 'Patient_id'},
                              'Specimen': {'left': 'DocumentReference_context_related_0_reference', 'right': 'Specimen_id'}}
    }

    def __init__(self, resource_list: list):
        self.resource_list = resource_list
        self.resource_dataframes = load_resources(resource_list)

    def join_resources(self):
        base_df_resource, *remaining_resources = self.resource_list
        base_df = add_resource_prefix(self.resource_dataframes.get(base_df_resource))
        joined_resources = [base_df_resource]
        
        for resource_to_join in remaining_resources:
            joining_df = add_resource_prefix(self.resource_dataframes.get(resource_to_join))

            mapping = FhirDataJoiner.foreign_key_mappings.get(joined_resources[-1]).get(resource_to_join)
            if mapping:
                left_key = mapping['left']
                right_key = mapping['right']

                if '_id' not in left_key:
                    base_df[left_key] = base_df[left_key].apply(strip_id_from_association)
                if '_id' not in right_key:
                    joining_df[right_key] = joining_df[right_key].apply(strip_id_from_association)

                base_df = base_df.merge(joining_df, 
                                        how='inner', 
                                        left_on=left_key, 
                                        right_on=right_key)

        return base_df


def reshape_fhir_combined_to_c2m2(the_df: pd.DataFrame, entity_name):
    """
    Reshape a Kids First combined table into a C2M2-formatted table for a given entity.

    Args:
        the_df (pd.DataFrame): A Kids First combined table.
        entity_name (str): The name of the entity for which the table is being reshaped.

    Returns:
        pd.DataFrame: A C2M2-formatted table for the given entity.
    """
    the_df = add_constants(ETLType.FHIR, the_df, c2m2_entity_name=entity_name)

    the_df.rename(columns=get_column_mappings(ETLType.FHIR, entity_name),inplace=True)

    the_df = the_df[get_table_cols_from_c2m2_json(entity_name)]

    return the_df

def strip_id_from_association(the_col):
    if pd.isna(the_col):
        return None
    elif isinstance(the_col,str) and len(the_col.split('/')) > 1:
        return int(the_col.split('/')[-1])
    else:
        return the_col

def load_resources(resource_list: list):
    fhir_resource_dataframe_dict = {}
    for resource in resource_list:
        if resource in fhir_resource_types:
            resource_path = os.path.join(file_locations.get_ingested_path(),f'{resource}.csv')
            the_df = pd.read_csv(resource_path,low_memory=False)
            fhir_resource_dataframe_dict.setdefault(resource,the_df) 
        else:
            raise ValueError(f'Fhir Resource not found for {resource}')

    return fhir_resource_dataframe_dict


def add_resource_prefix(the_df: pd.DataFrame):
    if 'resourceType' in the_df.columns:
        resource = the_df.at[the_df['resourceType'].first_valid_index(),'resourceType']
        the_df = the_df.add_prefix(f'{resource}_')

    return the_df 

def get_fhir_table_for_column(target_col: str):
    try:
        # Iterate over TSV files in the directory
        for filename in os.listdir(file_locations.get_fhir_mapping_paths()):
            if filename.endswith(".tsv"):
                file_path = os.path.join(file_locations.get_fhir_mapping_paths(), filename)
                df = pd.read_csv(file_path,sep='\t')
                if target_col in df['FHIR Field'].values:
                    return df 
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None