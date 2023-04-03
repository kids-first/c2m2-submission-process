import os
import pandas as pd
from typing import List

from cfde_convert import kf_to_cfde_value_converter, uberon_mapping_dict
from table_ops import TableJoiner, reshape_kf_combined_to_c2m2, project_title_row


etl_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl')
ingested_path = os.path.join(etl_path,'ingested') 
transformed_path = os.path.join(etl_path,'transformed') 
conversion_path = os.path.join(etl_path,'conversion_tables') 


def main():
    prepare_transformed_directory()

    convert_kf_to_project()
    
    convert_kf_to_project_in_project()

    kf_participants = get_kf_visible_participants()

    convert_kf_to_subject(kf_parts=kf_participants)

    convert_kf_to_biosample(kf_parts=kf_participants)

    convert_kf_to_biosample_from_subject(kf_parts=kf_participants)

    convert_kf_to_subject_disease(kf_parts=kf_participants)

    convert_kf_to_biosample_disease(kf_parts=kf_participants)

    convert_kf_to_subject_role_taxonomy(kf_parts=kf_participants)

    kf_genomic_files = get_kf_genomic_files(kf_parts=kf_participants)

    kf_genomic_files = convert_kf_to_file(kf_genomic_files)

    convert_kf_to_file_describes_biosample(kf_genomic_files)


def convert_kf_to_project():
    studies_df = pd.read_csv(os.path.join(ingested_path,'study.csv'))
    studies_on_portal_df = pd.read_table(os.path.join(etl_path,'studies_on_portal.txt'))

    project_df = TableJoiner(studies_df) \
                .join_kf_table(studies_on_portal_df,
                            left_key='SD_kf_id',
                            right_key='studies_on_portal') \
                .get_result()

    project_df['abbreviation'] = project_df['SD_kf_id']

    project_df = reshape_kf_combined_to_c2m2(project_df,'project')
    project_df = project_df.append(project_title_row,ignore_index=True)

    project_df.sort_values(by=['local_id'],ascending=False,inplace=True)
    project_df.to_csv(os.path.join(transformed_path,'project.tsv'),sep='\t',index=False)


def convert_kf_to_project_in_project():
    studies_df = pd.read_csv(os.path.join(ingested_path,'study.csv'))
    studies_on_portal_df = pd.read_table(os.path.join(etl_path,'studies_on_portal.txt'))

    project_in_project_df = TableJoiner(studies_df) \
                            .join_kf_table(studies_on_portal_df,
                                        left_key='SD_kf_id',
                                        right_key='studies_on_portal') \
                            .get_result()

    project_in_project_df = reshape_kf_combined_to_c2m2(project_in_project_df,'project_in_project')

    project_in_project_df.sort_values(by=['child_project_local_id'],ascending=False,inplace=True)
    project_in_project_df.to_csv(os.path.join(transformed_path,'project_in_project.tsv'),sep='\t',index=False)


def get_kf_visible_participants():
    kf_participant_df = pd.read_csv(os.path.join(ingested_path,'participant.csv')).query('visible == True')
    studies_df = pd.read_table(os.path.join(etl_path,'studies_on_portal.txt'))

    kf_participants = TableJoiner(kf_participant_df) \
                    .join_kf_table(studies_df,
                                   left_key='PT_study_id',
                                   right_key='studies_on_portal') \
                    .get_result()

    return kf_participants


def convert_kf_to_subject(kf_parts: pd.DataFrame):
    subject_df = kf_to_cfde_value_converter(kf_parts,'PT_gender')
    subject_df = kf_to_cfde_value_converter(subject_df,'PT_ethnicity')

    subject_df = reshape_kf_combined_to_c2m2(subject_df,'subject')
    
    subject_df.sort_values(by=['local_id'],inplace=True)
    subject_df.to_csv(os.path.join(transformed_path,'subject.tsv'),sep='\t',index=False)


def apply_uberon_mapping(source_text, uberon_id):

    if isinstance(uberon_id,str) and uberon_id.lower().startswith('uberon'):
        return uberon_id
    elif isinstance(source_text,str):
        for anatomy_term, id in uberon_mapping_dict.items():
            if anatomy_term in source_text.lower():
                return id.upper()

#requires additional work for anatomy
def convert_kf_to_biosample(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False).query('visible == True')
    
    biosample_df = TableJoiner(kf_parts) \
                .join_kf_table(biospec_df,
                            left_key='PT_kf_id',
                            right_key='BS_participant_id') \
                .get_result()

    biosample_df['BS_uberon_id_anatomical_site'] = biosample_df.apply(lambda the_df: 
                                                                    apply_uberon_mapping(the_df['BS_source_text_anatomical_site'],
                                                                                         the_df['BS_uberon_id_anatomical_site']),
                                                                    axis=1)

    biosample_df = reshape_kf_combined_to_c2m2(biosample_df,'biosample')

    biosample_df.sort_values(by=['local_id'],ascending=True,inplace=True)
    biosample_df.drop_duplicates(inplace=True)
    biosample_df.to_csv(os.path.join(transformed_path,'biosample.tsv'),sep='\t',index=False)


def convert_days_to_years(days):
    if days:
        return f"{(days / 365):.02f}"


def convert_kf_to_biosample_from_subject(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False).query('visible == True')

    biosample_from_subject_df = TableJoiner(kf_parts) \
                                .join_kf_table(biospec_df,
                                            left_key='PT_kf_id',
                                            right_key='BS_participant_id') \
                                .get_result()

    biosample_from_subject_df['BS_age_at_event_days'] = biosample_from_subject_df['BS_age_at_event_days'].apply(convert_days_to_years)

    biosample_from_subject_df = reshape_kf_combined_to_c2m2(biosample_from_subject_df,'biosample_from_subject')

    biosample_from_subject_df.sort_values(by=['biosample_local_id'],inplace=True)
    biosample_from_subject_df.drop_duplicates(inplace=True)
    biosample_from_subject_df.to_csv(os.path.join(transformed_path,'biosample_from_subject.tsv'),sep='\t',index=False)
    

def convert_kf_to_biosample_disease(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False).query('visible == True')
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    biosample_disease_df = TableJoiner(kf_parts) \
                        .join_kf_table(disease_mapping_df,
                                       left_key='PT_study_id',
                                       right_key='study_id') \
                        .join_kf_table(biospec_df,
                                       left_key='PT_kf_id',
                                       right_key='BS_participant_id') \
                        .get_result()

    # Ontology mapping not identified for this study
    biosample_disease_df.drop(biosample_disease_df.query('PT_study_id == "SD_DZ4GPQX6"').index,inplace=True)

    biosample_disease_df = reshape_kf_combined_to_c2m2(biosample_disease_df,'biosample_disease')

    biosample_disease_df.drop_duplicates(inplace=True)
    biosample_disease_df.sort_values(by=['biosample_local_id'],inplace=True)
    biosample_disease_df.to_csv(os.path.join(transformed_path,'biosample_disease.tsv'),sep='\t',index=False)


def convert_kf_to_subject_disease(kf_parts: pd.DataFrame) -> None:
    disease_mapping_df = pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv'))

    subject_disease_df = TableJoiner(kf_parts) \
                        .join_kf_table(disease_mapping_df,
                                       left_key='PT_study_id',
                                       right_key='study_id') \
                        .get_result()

    # Ontology mapping not identified for this study
    subject_disease_df.drop(subject_disease_df.query('PT_study_id == "SD_DZ4GPQX6"').index,inplace=True)

    subject_disease_df = reshape_kf_combined_to_c2m2(subject_disease_df, 'subject_disease')

    subject_disease_df.drop_duplicates(inplace=True)
    subject_disease_df.sort_values(by=['subject_local_id'],inplace=True)
    subject_disease_df.to_csv(os.path.join(transformed_path,'subject_disease.tsv'),sep='\t',index=False)


def convert_kf_to_subject_role_taxonomy(kf_parts: pd.DataFrame):
    subject_role_taxonomy_df = kf_parts.copy(deep=True)

    subject_role_taxonomy_df = reshape_kf_combined_to_c2m2(subject_role_taxonomy_df,'subject_role_taxonomy')

    subject_role_taxonomy_df.sort_values(by=['subject_local_id'],inplace=True)
    subject_role_taxonomy_df.drop_duplicates(inplace=True)
    subject_role_taxonomy_df.to_csv(os.path.join(transformed_path,'subject_role_taxonomy.tsv'),sep='\t',index=False)


def get_kf_genomic_files(kf_parts: pd.DataFrame):
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False).query('visible == True')
    biospec_genomic_df = pd.read_csv(os.path.join(ingested_path,'biospecimen_genomic_file.csv'),low_memory=False).query('visible == True')
    genomic_file_df = pd.read_csv(os.path.join(ingested_path,'genomic_file.csv'),low_memory=False).query('visible == True')
    
    genomic_file_df = TableJoiner(kf_parts) \
                    .join_kf_table(biospec_df,
                                left_key='PT_kf_id',
                                right_key='BS_participant_id') \
                    .join_kf_table(biospec_genomic_df,
                                left_key='BS_kf_id',
                                right_key='BG_biospecimen_id') \
                    .join_kf_table(genomic_file_df,
                                left_key='BG_genomic_file_id',
                                right_key='GF_kf_id') \
                    .get_result()

    genomic_file_df.sort_values(by='GF_kf_id',inplace=True)
    return genomic_file_df

def modify_dbgap(study_id):
    if study_id and isinstance(study_id, str):
        if study_id.startswith('phs'):
            return study_id.split('.')[0]
        else:
            return ''

def get_persistent_id(study, did, md5):
    if isinstance(study,str) and isinstance(did,str) and pd.notnull(md5):
        if study not in ['SD_BHJXBDQK','SD_8Y99QZJJ','SD_46RR9ZR6','SD_YNSSAPHE']:
            return 'drs://data.kidsfirstdrc.org/' + did

def path_to_filename(path):
    if isinstance(path,str):
        return path.split('/')[-1]

def convert_kf_to_file(kf_genomic_files: pd.DataFrame):

    # Omitted due to duplicate experiment strategies per genomic file
    # seq_experiment_gf_df = pd.read_csv(os.path.join(ingested_path,'sequencing_experiment_genomic_file.csv'),low_memory=False).query('visible == True')
    # seq_experiment_df = pd.read_csv(os.path.join(ingested_path,'sequencing_experiment.csv'),low_memory=False).query('visible == True')

    indexd_df = pd.read_csv(os.path.join(ingested_path,'indexd_scrape.csv'),low_memory=False)
    hashes_df = pd.read_csv(os.path.join(ingested_path,'hashes_old.csv'),low_memory=False)
    aws_scrape_df = pd.read_csv(os.path.join(ingested_path,'aws_scrape.csv'),low_memory=False)
    
    # Omitted due to duplicate experiment strategies per genomic file
    # with_seq_df = TableJoiner(kf_genomic_files) \
    #                     .left_join(seq_experiment_gf_df,
    #                                left_key='GF_kf_id',
    #                                right_key='SG_genomic_file_id') \
    #                     .join_kf_table(seq_experiment_df,
    #                                    left_key='SG_sequencing_experiment_id',
    #                                    right_key='SE_kf_id') \
    #                     .get_result()

    metadata_df = TableJoiner(indexd_df) \
            .join_kf_table(hashes_df,
                           left_key='url',
                           right_key='s3path') \
            .join_kf_table(aws_scrape_df,
                           left_key='url',
                           right_key='s3path') \
            .get_result()                   

    genomic_file_df = TableJoiner(kf_genomic_files) \
                    .left_join(metadata_df,
                               left_key='GF_latest_did',
                               right_key='did') \
                    .get_result()


    file_df = kf_to_cfde_value_converter(genomic_file_df,'GF_file_format')
    file_df = kf_to_cfde_value_converter(file_df,'GF_data_type')
    # Omitted due to duplicate experiment strategies per genomic file
    #file_df = kf_to_cfde_value_converter(file_df,'SE_experiment_strategy')
    file_df['BS_dbgap_consent_code'] = file_df['BS_dbgap_consent_code'].apply(modify_dbgap)

    file_df['persistent_id'] = file_df.apply(lambda the_df: 
                                             get_persistent_id(the_df['PT_study_id'],
                                                               the_df['GF_latest_did'],
                                                               the_df['md5']),
                                             axis=1)

    file_df['filename'] = file_df['GF_external_id'].apply(path_to_filename)

    file_df = reshape_kf_combined_to_c2m2(file_df,'file')

    file_df.drop_duplicates(inplace=True)
    file_df.sort_values(by=['local_id'],inplace=True)
    file_df.to_csv(os.path.join(transformed_path,'file.tsv'),sep='\t',index=False)
    return genomic_file_df


def convert_kf_to_file_describes_biosample(kf_genomic_files: pd.DataFrame):

    file_describes_biosample_df = reshape_kf_combined_to_c2m2(kf_genomic_files,'file_describes_biosample')

    file_describes_biosample_df.sort_values(by=['biosample_local_id','file_local_id'],inplace=True)

    file_describes_biosample_df.drop_duplicates(inplace=True)
    file_describes_biosample_df.to_csv(os.path.join(transformed_path,'file_describes_biosample.tsv'),sep='\t',index=False)


def prepare_transformed_directory():
    try:
        os.mkdir(transformed_path)
    except:
        print('Transformed directory already exists.... Skipping directory creation.')


if __name__ == "__main__":
    main()