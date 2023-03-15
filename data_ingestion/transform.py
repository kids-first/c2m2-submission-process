import os
import pandas as pd
from typing import List

from cfde_table_constants import get_table_cols_from_c2m2_json, get_column_mappings, add_constants
from cfde_convert import kf_to_cfde_subject_value_converter
from table_ops import TableJoiner, reshape_kf_combined_to_c2m2


ingestion_path = os.path.join(os.getcwd(),'data_ingestion')
ingested_path = os.path.join(ingestion_path,'ingested') 
transformed_path = os.path.join(ingestion_path,'transformed') 
conversion_path = os.path.join(ingestion_path,'conversion_tables') 


def main():
    prepare_transformed_directory()

    get_project()
    
    get_project_in_project()

    kf_participants = get_kf_visible_participants()

    get_subject(kf_parts=kf_participants)

    get_biosample(kf_parts=kf_participants)

    get_biosample_from_subject(kf_parts=kf_participants)

    get_subject_disease(kf_parts=kf_participants)

    get_biosample_disease(kf_parts=kf_participants)

    get_subject_role_taxonomy(kf_parts=kf_participants)

    get_file_describes_biosample(kf_participants)


def get_project():
    studies_df = pd.read_csv(os.path.join(ingested_path,'study.csv'))
    studies_on_portal_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    project_df = TableJoiner(studies_df) \
                .join_table(studies_on_portal_df,
                            left_key='kf_id',
                            right_key='studies_on_portal') \
                .get_result()

    project_df['abbreviation'] = project_df['kf_id']

    project_df = reshape_kf_combined_to_c2m2(project_df,'project')

    project_df.sort_values(by=['local_id'],ascending=False,inplace=True)
    project_df.to_csv(os.path.join(transformed_path,'project.tsv'),sep='\t',index=False)


def get_project_in_project():
    studies_df = pd.read_csv(os.path.join(ingested_path,'study.csv'))
    studies_on_portal_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    project_in_project_df = TableJoiner(studies_df) \
                            .join_table(studies_on_portal_df,
                                        left_key='kf_id',
                                        right_key='studies_on_portal') \
                            .get_result()

    project_in_project_df = reshape_kf_combined_to_c2m2(project_in_project_df,'project_in_project')

    project_in_project_df.sort_values(by=['child_project_local_id'],ascending=False,inplace=True)
    project_in_project_df.to_csv(os.path.join(transformed_path,'project_in_project.tsv'),sep='\t',index=False)


def get_kf_visible_participants():
    kf_participant_df = pd.read_csv(os.path.join(ingested_path,'participant.csv'))
    studies_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    kf_participants = kf_participant_df.merge(studies_df,
                                                    how='inner',
                                                    left_on='study_id',
                                                    right_on='studies_on_portal')

    return kf_participants


def get_subject(kf_parts: pd.DataFrame):
    subject_df = kf_to_cfde_subject_value_converter(kf_parts,'gender')
    subject_df = kf_to_cfde_subject_value_converter(subject_df,'ethnicity')

    subject_df = reshape_kf_combined_to_c2m2(subject_df,'subject')
    
    subject_df.sort_values(by=['local_id'],inplace=True)
    subject_df.to_csv(os.path.join(transformed_path,'subject.tsv'),sep='\t',index=False)


#requires additional work for anatomy
def get_biosample(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)
    
    biosample_df = TableJoiner(kf_parts) \
                .join_table(biospec_df,
                            left_key='kf_id',
                            right_key='participant_id') \
                .get_result()

    biosample_df = reshape_kf_combined_to_c2m2(biosample_df,'biosample')

    biosample_df.sort_values(by=['local_id'],ascending=True,inplace=True)
    biosample_df.to_csv(os.path.join(transformed_path,'biosample.tsv'),sep='\t',index=False)


def convert_days_to_years(days):
    if days:
        return f"{(days / 365):.02f}"


def get_biosample_from_subject(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)

    biosample_from_subject_df = TableJoiner(kf_parts) \
                                .join_table(biospec_df,
                                            left_key='kf_id',
                                            right_key='participant_id') \
                                .get_result()

    biosample_from_subject_df['age_at_event_days'] = biosample_from_subject_df['age_at_event_days'].apply(convert_days_to_years)

    biosample_from_subject_df = reshape_kf_combined_to_c2m2(biosample_from_subject_df,'biosample_from_subject')

    biosample_from_subject_df.sort_values(by=['biosample_local_id'],inplace=True)

    biosample_from_subject_df.to_csv(os.path.join(transformed_path,'biosample_from_subject.tsv'),sep='\t',index=False)
    

def get_biosample_disease(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    biosample_disease_df = TableJoiner(kf_parts) \
                        .join_table(disease_mapping_df,
                                    left_key='study_id') \
                        .join_table(biospec_df,
                                    left_key='kf_id',
                                    right_key='participant_id') \
                        .get_result()

    biosample_disease_df = kf_to_cfde_subject_value_converter(biosample_disease_df,'source_text_tissue_type')

    biosample_disease_df = reshape_kf_combined_to_c2m2(biosample_disease_df,'biosample_disease')

    biosample_disease_df.drop_duplicates(inplace=True)
    biosample_disease_df.sort_values(by=['biosample_local_id'],inplace=True)
    biosample_disease_df.to_csv(os.path.join(transformed_path,'biosample_disease.tsv'),sep='\t',index=False)


def get_subject_disease(kf_parts: pd.DataFrame) -> None:
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    subject_disease_df = TableJoiner(kf_parts) \
                        .join_table(disease_mapping_df,
                                    left_key='study_id') \
                        .get_result()

    subject_disease_df = reshape_kf_combined_to_c2m2(subject_disease_df, 'subject_disease')

    subject_disease_df.drop_duplicates(inplace=True)
    subject_disease_df.sort_values(by=['subject_local_id'],inplace=True)
    subject_disease_df.to_csv(os.path.join(transformed_path,'subject_disease.tsv'),sep='\t',index=False)


def get_subject_role_taxonomy(kf_parts: pd.DataFrame):
    subject_role_taxonomy_df = kf_parts.copy(deep=True)

    subject_role_taxonomy_df = reshape_kf_combined_to_c2m2(subject_role_taxonomy_df,'subject_role_taxonomy')

    subject_role_taxonomy_df.sort_values(by=['subject_local_id'],inplace=True)
    subject_role_taxonomy_df.to_csv(os.path.join(transformed_path,'subject_role_taxonomy.tsv'),sep='\t',index=False)


def get_file_describes_biosample(kf_participants: pd.DataFrame):
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)
    biospec_genomic_file_df = pd.read_csv(os.path.join(ingested_path,'biospecimen_genomic_file.csv'),low_memory=False)
    genomic_file_df = pd.read_csv(os.path.join(ingested_path,'genomic_file.csv'),low_memory=False)

    file_describes_biosample_df = TableJoiner(kf_participants) \
                                .join_table(biospec_df,
                                            left_key='kf_id',
                                            right_key='participant_id') \
                                .join_table(biospec_genomic_file_df,
                                            left_key='kf_id',
                                            right_key='biospecimen_id'
                                                    ) \
                                .join_table(genomic_file_df,
                                            left_key='genomic_file_id',
                                            right_key='kf_id') \
                                .get_result()

    file_describes_biosample_df = reshape_kf_combined_to_c2m2(file_describes_biosample_df,'file_describes_biosample')

    file_describes_biosample_df.sort_values(by=['biosample_local_id','file_local_id'],inplace=True)

    file_describes_biosample_df.to_csv(os.path.join(transformed_path,'file_describes_biosample.tsv'),sep='\t',index=False)


def prepare_transformed_directory():
    try:
        os.mkdir(transformed_path)
    except:
        print('Transformed directory already exists.... Skipping directory creation.')


if __name__ == "__main__":
    main()