import os
import pandas as pd
from typing import List

from cfde_table_constants import get_table_cols_from_c2m2_json
from cfde_convert import gender_to_cfde_subject_sex, ethnicity_to_cfde_subject_ethnicity, tissue_type_to_cfde_disease_association


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

    studies_df['id_namespace'] = id_namespace
    studies_df['persistent_id'] = ''
    studies_df['creation_time'] = '' 
    studies_df['abbreviation'] = studies_df['kf_id']
    studies_df.rename(columns={'kf_id':'local_id','short_name':'name','name':'description'},inplace=True)

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

    studies_df['parent_project_id_namespace'] = id_namespace
    studies_df['parent_project_local_id'] = 'drc'
    studies_df['child_project_id_namespace'] = id_namespace
    studies_df['child_project_local_id'] = studies_df['kf_id']

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

    visible_pariticipants['id_namespace'] = 'kidsfirst:'
    visible_pariticipants['project_id_namespace'] = 'kidsfirst:'
    visible_pariticipants['persistent_id'] = ' '
    visible_pariticipants['granularity'] = ' '
    visible_pariticipants['age_at_enrollment'] = ' '

    visible_pariticipants = gender_to_cfde_subject_sex(visible_pariticipants)
    visible_pariticipants['gender'].fillna('cfde_subject_sex:0',inplace=True)
    visible_pariticipants = ethnicity_to_cfde_subject_ethnicity(visible_pariticipants)

    visible_pariticipants.rename(columns={'kf_id':'local_id',
                                          'gender':'sex',
                                          'study_id':'project_local_id',
                                          'created_at':'creation_time'},inplace=True) 

    visible_pariticipants = visible_pariticipants[get_table_cols_from_c2m2_json('subject')]
    
    visible_pariticipants.sort_values(by=['local_id'],inplace=True)
    visible_pariticipants.to_csv(os.path.join(transformed_path,'subject.tsv'),sep='\t',index=False)

    return visible_pariticipants


def get_biosample(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)

    kf_biospec_df = biospec_df.merge(kf_parts,
                                     how='inner',
                                     left_on='participant_id',
                                     right_on='local_id')

    kf_biospec_df['id_namespace'] = id_namespace
    kf_biospec_df['project_id_namespace'] = project_id_namespace
    kf_biospec_df['sample_prep_method'] = ' '

    kf_biospec_df.drop(columns=['local_id'],inplace=True)
    kf_biospec_df.rename(columns={'kf_id':'local_id',
                                  'created_at_x':'created_at',
                                  'uberon_id_anatomical_site':'anatomy'},
                                  inplace=True)

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

    kf_biospec_df['biosample_id_namespace'] = 'kidsfirst:'
    kf_biospec_df['subject_id_namespace'] = 'kidsfirst:'
    kf_biospec_df['age_at_event_days'] = kf_biospec_df['age_at_event_days'].apply(convert_days_to_years)


    kf_biospec_df.rename(columns={'kf_id':'biosample_local_id',
                                  'local_id':'subject_local_id',
                                  'age_at_event_days':'age_at_sampling'},
                                  inplace=True)

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

    biosample_disease_df['biosample_id_namespace'] = id_namespace

    biosample_disease_df = tissue_type_to_cfde_disease_association(biosample_disease_df)

    biosample_disease_df.rename(columns={'kf_id':'biosample_local_id',
                                  'created_at_x':'created_at',
                                  'source_text_tissue_type':'association_type',
                                  'DOID':'disease'},
                                  inplace=True)

    biosample_disease_df = biosample_disease_df[get_table_cols_from_c2m2_json('biosample_disease')]

    biosample_disease_df.drop_duplicates(inplace=True)
    biosample_disease_df.sort_values(by=['biosample_local_id'],inplace=True)
    biosample_disease_df.to_csv(os.path.join(transformed_path,'biosample_disease.tsv'),sep='\t',index=False)


def get_subject_disease(kf_parts: pd.DataFrame) -> None:
    diagnosis_df = pd.read_csv(os.path.join(ingested_path,'diagnosis.csv'),low_memory=False)
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    kf_parts_diseased = kf_parts.merge(disease_mapping_df,
                                       how='inner',
                                       left_on='project_local_id',
                                       right_on='study_id').drop(columns=['study_id','DOID_description'])


    kf_parts_diseased['subject_id_namespace'] = id_namespace
    kf_parts_diseased['association_type'] = 'cfde_disease_association_type:1'


    kf_parts_diseased.rename(columns={'local_id':'subject_local_id',
                                  'created_at_x':'created_at',
                                  'DOID':'disease'},
                                  inplace=True)

    kf_parts_diseased = kf_parts_diseased[get_table_cols_from_c2m2_json('subject_disease')]

    kf_parts_diseased.drop_duplicates(inplace=True)
    kf_parts_diseased.sort_values(by=['subject_local_id'],inplace=True)
    kf_parts_diseased.to_csv(os.path.join(transformed_path,'subject_disease.tsv'),sep='\t',index=False)


def get_subject_role_taxonomy(kf_parts: pd.DataFrame):
    subject_role_taxonomy_df = pd.DataFrame()
    subject_role_taxonomy_df['subject_local_id'] = kf_parts['local_id']
    subject_role_taxonomy_df['subject_id_namespace'] = id_namespace
    subject_role_taxonomy_df['role_id'] = 'cfde_subject_role:0'
    subject_role_taxonomy_df['taxonomy_id'] = 'NCBI:txid9606'

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