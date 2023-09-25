import os

import pandas as pd
import numpy as np

from fhir_table_joiner import FhirDataJoiner, reshape_fhir_combined_to_c2m2
from cfde_convert import kf_to_cfde_value_converter
from file_locations import file_locations
from etl_types import ETLType
from value_converter import convert_days_to_years, modify_dbgap, \
                            apply_uberon_mapping


def transform_fhir_to_c2m2_on_disk():
    convert_fhir_to_project_in_project(fhir_combined_list= ['ResearchStudy'],
                            c2m2_entity_name = 'project_in_project',
                            sort_on='child_project_local_id',
                            ascending_sort=False)

    convert_fhir_to_project(fhir_combined_list= ['ResearchStudy'],
                            c2m2_entity_name = 'project',
                            sort_on='local_id',
                            ascending_sort=False)

    convert_fhir_to_subject(fhir_combined_list = ['Patient'],
                               c2m2_entity_name = 'subject',
                               sort_on='local_id',
                               ascending_sort=False)

    convert_fhir_to_biosample(fhir_combined_list = ['Specimen'],
                               c2m2_entity_name = 'biosample',
                               sort_on='local_id',
                               ascending_sort=False)

    convert_fhir_to_biosample_from_subject(fhir_combined_list = ['Patient','Specimen'],
                               c2m2_entity_name = 'biosample_from_subject',
                               sort_on='biosample_local_id',
                               ascending_sort=False)

    convert_fhir_to_subject_disease(fhir_combined_list = ['Patient'],
                               c2m2_entity_name = 'subject_disease',
                               sort_on='subject_local_id',
                               ascending_sort=False)

    convert_fhir_to_biosample_disease(fhir_combined_list=['Specimen'],
                                    c2m2_entity_name='biosample_disease',
                                    sort_on='biosample_local_id',
                                    ascending_sort=True)

    convert_fhir_to_subject_role_taxonomy(fhir_combined_list=['Patient'],
                                        c2m2_entity_name='subject_role_taxonomy',
                                        sort_on='subject_local_id',
                                        ascending_sort=True)

    convert_fhir_to_file(fhir_combined_list=['Specimen','DocumentReference'],
                         c2m2_entity_name='file',
                         sort_on='local_id',
                         ascending_sort=True)

    convert_fhir_to_file_describes_biosample(fhir_combined_list=['Specimen','DocumentReference'],
                                          c2m2_entity_name='file_describes_biosample',
                                          sort_on=['biosample_local_id','file_local_id'],
                                          ascending_sort=True)


def convert_fhir_to_c2m2(func):
    def wrapper(**kwargs):
        fhir_combined_df = FhirDataJoiner(kwargs['fhir_combined_list']).join_resources()
        combined_adjusted = func(fhir_combined_df)
        c2m2_df = reshape_fhir_combined_to_c2m2(combined_adjusted,kwargs['c2m2_entity_name'])
        
        c2m2_df.sort_values(by=kwargs['sort_on'],
                            ascending=kwargs['ascending_sort'],
                            inplace=True)

        c2m2_df.drop_duplicates(inplace=True)
        c2m2_df.to_csv(os.path.join(file_locations.get_transformed_path(),f'{kwargs["c2m2_entity_name"]}.tsv'),
                       sep='\t',
                       index=False)
    return wrapper


@convert_fhir_to_c2m2
def convert_fhir_to_project_in_project(the_df: pd.DataFrame):
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_project(the_df: pd.DataFrame):
    the_df['abbreviation'] = the_df['identifier_0_value']
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_subject(the_df: pd.DataFrame):
    the_df = kf_to_cfde_value_converter(ETLType.FHIR, the_df, 'gender')
    the_df = kf_to_cfde_value_converter(ETLType.FHIR, the_df, 'extension_1_extension_0_valueString')
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_biosample(the_df: pd.DataFrame):
    the_df['collection_bodySite_text'] = the_df.apply(lambda the_df: 
                                                          apply_uberon_mapping(ETLType.FHIR,the_df['collection_bodySite_text']),
                                                          axis=1)
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_biosample_from_subject(the_df: pd.DataFrame):
    print('Converting kf to c2m2 biosample from subject')
    age_at_even_days_col_name = 'Specimen_collection__collectedDateTime_extension_0_extension_3_valueDuration_value'
    if age_at_even_days_col_name in the_df.columns:
        the_df[age_at_even_days_col_name] = the_df[age_at_even_days_col_name].apply(convert_days_to_years)
    else:
        the_df[age_at_even_days_col_name] = None
    return the_df

def remove_studies_without_disease_mapping(the_df: pd.DataFrame):
    disease_mapping_df = pd.read_table(os.path.join(file_locations.get_ontology_mappings_path(),'project_disease_matrix_only.tsv'))
    no_disease_df = disease_mapping_df.loc[(disease_mapping_df['DOID'] == 'NA') | (disease_mapping_df['DOID'].isna())]
    return the_df[~the_df['meta_tag_0_code'].isin(no_disease_df['study_id'])]

@convert_fhir_to_c2m2
def convert_fhir_to_subject_disease(the_df: pd.DataFrame):
    project_disease_df = pd.read_table(os.path.join(file_locations.get_ontology_mappings_path(),'project_disease_matrix_only.tsv'))

    the_df = the_df.merge(project_disease_df,
                          how='inner',
                          left_on='meta_tag_0_code',
                          right_on='study_id')

    the_df = remove_studies_without_disease_mapping(the_df)

    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_biosample_disease(the_df: pd.DataFrame):
    project_disease_df = pd.read_table(os.path.join(file_locations.get_ontology_mappings_path(),'project_disease_matrix_only.tsv'))

    the_df = the_df.merge(project_disease_df,
                          how='inner',
                          left_on='meta_tag_0_code',
                          right_on='study_id')

    the_df = remove_studies_without_disease_mapping(the_df)

    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_subject_role_taxonomy(the_df: pd.DataFrame):
    return the_df

def update_hash(row):
    if row['DocumentReference_content_0_attachment_extension_1_valueCodeableConcept_coding_0_display'] == 'etag':
        return np.nan
    else:
        return row['DocumentReference_content_0_attachment_extension_1_valueCodeableConcept_text']

def update_persistent_id(row):
    if row['DocumentReference_content_0_attachment_extension_1_valueCodeableConcept_coding_0_display'] == 'etag':
        return np.nan
    else:
        return row['DocumentReference_content_0_attachment_url']

@convert_fhir_to_c2m2
def convert_fhir_to_file(the_df: pd.DataFrame):
    the_df = kf_to_cfde_value_converter(ETLType.FHIR, the_df, 'DocumentReference_content_0_format_display')
    the_df = kf_to_cfde_value_converter(ETLType.FHIR, the_df, 'DocumentReference_type_coding_0_code')
    the_df = kf_to_cfde_value_converter(ETLType.FHIR, the_df, 'DocumentReference_category_0_text')

    the_df['Specimen_meta_security_1_code'] = the_df['Specimen_meta_security_1_code'].apply(modify_dbgap)

    the_df['DocumentReference_content_0_attachment_extension_1_valueCodeableConcept_text'] = the_df.apply(update_hash,axis=1)
    the_df['DocumentReference_content_0_attachment_url'] = the_df.apply(update_persistent_id,axis=1)

    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_file_describes_biosample(the_df: pd.DataFrame):
    return the_df

if __name__ == "__main__":
    transform_fhir_to_c2m2_on_disk()