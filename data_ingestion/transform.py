import os
import pandas as pd
from typing import List

from cfde_table_constants import get_table_cols_from_c2m2_json, get_column_mappings, add_constants
from cfde_convert import kf_to_cfde_subject_value_converter 


ingestion_path = os.path.join(os.getcwd(),'data_ingestion')
ingested_path = os.path.join(ingestion_path,'ingested') 
transformed_path = os.path.join(ingestion_path,'transformed') 
conversion_path = os.path.join(ingestion_path,'conversion_tables') 

id_namespace = 'kidsfirst:'
project_id_namespace = 'kidsfirst:'

def main():
    prepare_transformed_directory()

    get_project()
    
    get_project_in_project()

    kf_participants = get_kf_visible_participants()

    get_biosample(kf_parts=kf_participants)

    get_biosample_from_subject(kf_parts=kf_participants)

    get_subject_disease(kf_parts=kf_participants)

    get_biosample_disease(kf_parts=kf_participants)

    get_subject_role_taxonomy(kf_parts=kf_participants)

def get_project():
    studies_df = pd.read_csv(os.path.join(ingested_path,'study.csv'))
    studies_on_portal_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    studies_df = studies_df.merge(studies_on_portal_df,
                                  how='inner',
                                  left_on='kf_id',
                                  right_on='studies_on_portal')

    studies_df = add_constants(studies_df,'project')
    studies_df['abbreviation'] = studies_df['kf_id']

    studies_df.rename(columns=get_column_mappings('project'),inplace=True)

    studies_df = studies_df[get_table_cols_from_c2m2_json('project')]

    studies_df.sort_values(by=['local_id'],ascending=False,inplace=True)
    studies_df.to_csv(os.path.join(transformed_path,'project.tsv'),sep='\t',index=False)


def get_project_in_project():
    studies_df = pd.read_csv(os.path.join(ingested_path,'study.csv'))
    studies_on_portal_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    studies_df = studies_df.merge(studies_on_portal_df,
                                  how='inner',
                                  left_on='kf_id',
                                  right_on='studies_on_portal')

    studies_df = add_constants(studies_df,'project_in_project') 

    studies_df.rename(columns=get_column_mappings('project_in_project'), inplace=True)

    studies_df = studies_df[get_table_cols_from_c2m2_json('project_in_project')]

    studies_df.sort_values(by=['child_project_local_id'],ascending=False,inplace=True)
    studies_df.to_csv(os.path.join(transformed_path,'project_in_project.tsv'),sep='\t',index=False)


def get_kf_visible_participants():
    kf_participant_df = pd.read_csv(os.path.join(ingested_path,'participant.csv'))
    studies_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    visible_pariticipants = kf_participant_df.merge(studies_df,
                                                    how='inner',
                                                    left_on='study_id',
                                                    right_on='studies_on_portal')

    visible_pariticipants = add_constants(visible_pariticipants,'subject')

    visible_pariticipants = kf_to_cfde_subject_value_converter(visible_pariticipants,'gender')
    visible_pariticipants = kf_to_cfde_subject_value_converter(visible_pariticipants,'ethnicity')

    visible_pariticipants.rename(columns=get_column_mappings('subject'),inplace=True) 

    visible_pariticipants = visible_pariticipants[get_table_cols_from_c2m2_json('subject')]
    
    visible_pariticipants.sort_values(by=['local_id'],inplace=True)
    visible_pariticipants.to_csv(os.path.join(transformed_path,'subject.tsv'),sep='\t',index=False)

    return visible_pariticipants


#requires additional work for anatomy
def get_biosample(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)

    kf_biospec_df = biospec_df.merge(kf_parts,
                                     how='inner',
                                     left_on='participant_id',
                                     right_on='local_id')

    kf_biospec_df = add_constants(kf_biospec_df,'biosample') 

    # Getting rid of conflicting subject cols
    kf_biospec_df.drop(columns=['local_id','creation_time'],inplace=True)

    kf_biospec_df.rename(columns=get_column_mappings('biosample'),inplace=True)

    kf_biospec_df = kf_biospec_df[get_table_cols_from_c2m2_json('biosample')]

    kf_biospec_df.sort_values(by=['local_id'],ascending=True,inplace=True)
    kf_biospec_df.to_csv(os.path.join(transformed_path,'biosample.tsv'),sep='\t',index=False)


def convert_days_to_years(days):
    if days:
        return f"{(days / 365):.02f}"

def get_biosample_from_subject(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)

    kf_biospec_df = biospec_df.merge(kf_parts,
                                     how='inner',
                                     left_on='participant_id',
                                     right_on='local_id')

    kf_biospec_df = add_constants(kf_biospec_df,'biosample_from_subject')
    kf_biospec_df['age_at_event_days'] = kf_biospec_df['age_at_event_days'].apply(convert_days_to_years)


    kf_biospec_df.rename(columns=get_column_mappings('biosample_from_subject'),inplace=True)

    kf_biospec_df = kf_biospec_df[get_table_cols_from_c2m2_json('biosample_from_subject')]
    kf_biospec_df.sort_values(by=['biosample_local_id'],inplace=True)

    kf_biospec_df.to_csv(os.path.join(transformed_path,'biosample_from_subject.tsv'),sep='\t',index=False)
    

def get_biosample_disease(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    kf_parts_diseased = kf_parts.merge(disease_mapping_df,
                                       how='inner',
                                       left_on='project_local_id',
                                       right_on='study_id').drop(columns=['study_id','DOID_description'])


    biosample_disease_df = kf_parts_diseased.merge(biospec_df,
                                                   how='inner',
                                                   left_on='local_id',
                                                   right_on='participant_id'
                                                   )

    add_constants(biosample_disease_df,'biosample_disease')

    biosample_disease_df = kf_to_cfde_subject_value_converter(biosample_disease_df,'source_text_tissue_type')

    biosample_disease_df.rename(columns=get_column_mappings('biosample_disease'),inplace=True)

    biosample_disease_df = biosample_disease_df[get_table_cols_from_c2m2_json('biosample_disease')]

    biosample_disease_df.drop_duplicates(inplace=True)
    biosample_disease_df.sort_values(by=['biosample_local_id'],inplace=True)
    biosample_disease_df.to_csv(os.path.join(transformed_path,'biosample_disease.tsv'),sep='\t',index=False)


def get_subject_disease(kf_parts: pd.DataFrame) -> None:
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    kf_parts_diseased = kf_parts.merge(disease_mapping_df,
                                       how='inner',
                                       left_on='project_local_id',
                                       right_on='study_id').drop(columns=['study_id','DOID_description'])


    add_constants(kf_parts_diseased,'subject_disease') 


    kf_parts_diseased.rename(columns=get_column_mappings('subject_disease'),inplace=True)

    kf_parts_diseased = kf_parts_diseased[get_table_cols_from_c2m2_json('subject_disease')]

    kf_parts_diseased.drop_duplicates(inplace=True)
    kf_parts_diseased.sort_values(by=['subject_local_id'],inplace=True)
    kf_parts_diseased.to_csv(os.path.join(transformed_path,'subject_disease.tsv'),sep='\t',index=False)


def get_subject_role_taxonomy(kf_parts: pd.DataFrame):
    subject_role_taxonomy_df = kf_parts.copy(deep=True)

    add_constants(subject_role_taxonomy_df,'subject_role_taxonomy')
    subject_role_taxonomy_df.rename(columns=get_column_mappings('subject_role_taxonomy'),inplace=True)

    subject_role_taxonomy_df = subject_role_taxonomy_df[get_table_cols_from_c2m2_json('subject_role_taxonomy')]

    subject_role_taxonomy_df.sort_values(by=['subject_local_id'],inplace=True)
    subject_role_taxonomy_df.to_csv(os.path.join(transformed_path,'subject_role_taxonomy.tsv'),sep='\t',index=False)

def prepare_transformed_directory():
    try:
        os.mkdir(transformed_path)
    except:
        print('Transformed directory already exists.... Skipping directory creation.')


if __name__ == "__main__":
    main()