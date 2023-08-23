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
        'DocumentReference': {'Patient': {'left': 'subject_reference', 'right': 'patient_id'},
                              'Specimen': {'left': 'subject_reference', 'right': 'specimen_id'}}
    }

    def __init__(self, resource_list):
        self.resource_list = resource_list
        self.resource_dataframes = load_resources(resource_list)

    def join_resources(self):
        if len(self.resource_list) < 2:
            only_resource = self.resource_list[0]
            print(f"Not enough resources to join. Returning {only_resource} resource.")
            return self.resource_dataframes.get(only_resource)

        joined_df = self.resource_dataframes.get(self.resource_list[0])
        if joined_df is None:
            raise ValueError(f"DataFrame not found for resource {self.resource_list[0]}")

        for i in range(1, len(self.resource_list)):
            resource_name = self.resource_list[i]
            df = self.resource_dataframes.get(resource_name)

            if df is None:
                raise ValueError(f"DataFrame not found for resource {resource_name}")

            mapping = FhirDataJoiner.foreign_key_mappings.get(self.resource_list[i - 1], {}).get(resource_name)
            if mapping:
                left_key = mapping['left']
                right_key = mapping['right']
                joined_df = add_resource_prefix(joined_df)
                df = add_resource_prefix(df)
                df[right_key] = df[right_key].apply(strip_id_from_association)
                joined_df = pd.merge(joined_df, df, left_on=left_key, right_on=right_key, how='inner')
            else:
                raise ValueError(f"No mapping found for joining {self.resource_list[i - 1]} and {resource_name}")

        return joined_df


def reshape_fhir_combined_to_c2m2(the_df: pd.DataFrame, entity_name):
    """
    Reshape a Kids First combined table into a C2M2-formatted table for a given entity.

    Args:
        the_df (pd.DataFrame): A Kids First combined table.
        entity_name (str): The name of the entity for which the table is being reshaped.

    Returns:
        pd.DataFrame: A C2M2-formatted table for the given entity.
    """
    the_df = add_constants(the_df, c2m2_entity_name=entity_name)

    the_df.rename(columns=get_column_mappings(ETLType.FHIR, entity_name),inplace=True)

    the_df = the_df[get_table_cols_from_c2m2_json(entity_name)]

    return the_df

def strip_id_from_association(the_col):
    if pd.isna(the_col):
        return None
    else:
        return int(the_col.split('/')[-1])

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