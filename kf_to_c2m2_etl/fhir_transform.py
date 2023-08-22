import os

import pandas as pd

from fhir_table_joiner import FhirDataJoiner, reshape_fhir_combined_to_c2m2
from file_locations import file_locations

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
    the_df['abbreviation'] = the_df['keyword_1_coding_0_code']
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_subject(the_df: pd.DataFrame):
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_biosample(the_df: pd.DataFrame):
    return the_df

def convert_days_to_years(days):
    if days:
        return f"{(days / 365):.02f}"

@convert_fhir_to_c2m2
def convert_fhir_to_biosample_from_subject(the_df: pd.DataFrame):
    print('Converting kf to c2m2 biosample from subject')
    age_at_even_days_col_name = 'Specimen_collection__collectedDateTime_extension_0_extension_3_valueDuration_value'
    if age_at_even_days_col_name in the_df.columns:
        the_df[age_at_even_days_col_name] = the_df[age_at_even_days_col_name].apply(convert_days_to_years)
    else:
        the_df[age_at_even_days_col_name] = None
    return the_df

@convert_fhir_to_c2m2
def convert_fhir_to_subject_disease(the_df: pd.DataFrame):
    project_disease_df = pd.read_table(os.path.join(file_locations.get_ontology_mappings_path(),'project_disease_matrix_only.tsv'))
    the_df = the_df.merge(project_disease_df,
                          how='inner',
                          left_on='meta_tag_0_code',
                          right_on='study_id')
    return the_df