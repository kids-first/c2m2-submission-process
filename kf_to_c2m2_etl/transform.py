import os
import pandas as pd
from typing import List

from cfde_convert import kf_to_cfde_value_converter, uberon_mapping_dict
from table_ops import TableJoiner, reshape_kf_combined_to_c2m2, project_title_row
from kf_table_combiner import KfTableCombiner


etl_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl')
ingested_path = os.path.join(etl_path,'ingested') 
transformed_path = os.path.join(etl_path,'transformed') 
conversion_path = os.path.join(etl_path,'conversion_tables') 


def main():
    prepare_transformed_directory()

    convert_kf_to_project(kf_combined_list=['portal_studies','study'],
                          c2m2_entity_name='project',
                          sort_on='local_id',
                          ascending_sort=False)
    
    convert_kf_to_project_in_project(kf_combined_list=['portal_studies','study'],
                                     c2m2_entity_name='project_in_project',
                                     sort_on='child_project_local_id',
                                     ascending_sort=False)

    convert_kf_to_subject(kf_combined_list=['portal_studies','participant'],
                          c2m2_entity_name='subject',
                          sort_on='local_id',
                          ascending_sort=True)

    convert_kf_to_biosample(kf_combined_list=['portal_studies','participant','biospecimen'],
                            c2m2_entity_name='biosample',
                            sort_on='local_id',
                            ascending_sort=True)

    convert_kf_to_biosample_from_subject(kf_combined_list=['portal_studies','participant','biospecimen'],
                                         c2m2_entity_name='biosample_from_subject',
                                         sort_on='biosample_local_id',
                                         ascending_sort=True)

    convert_kf_to_subject_disease(kf_combined_list=['portal_studies','participant','project_disease'],
                                  c2m2_entity_name='subject_disease',
                                  sort_on='subject_local_id',
                                  ascending_sort=True)

    convert_kf_to_biosample_disease(kf_combined_list=['portal_studies','participant','biospecimen','project_disease'],
                                    c2m2_entity_name='biosample_disease',
                                    sort_on='biosample_local_id',
                                    ascending_sort=True)

    convert_kf_to_subject_role_taxonomy(kf_combined_list=['portal_studies','participant'],
                                        c2m2_entity_name='subject_role_taxonomy',
                                        sort_on='subject_local_id',
                                        ascending_sort=True)

    kf_genomic_files = get_kf_genomic_files()

    kf_genomic_files = convert_kf_to_file(kf_genomic_files)

    convert_kf_to_file_describes_biosample(kf_genomic_files)

def convert_kf_to_c2m2(func):
    def wrapper(**kwargs):
        kf_combined_df = KfTableCombiner(kwargs['kf_combined_list']).get_combined_table()
        combined_adjusted = func(kf_combined_df)
        c2m2_df = reshape_kf_combined_to_c2m2(combined_adjusted,kwargs['c2m2_entity_name'])
        
        c2m2_df.sort_values(by=kwargs['sort_on'],
                            ascending=kwargs['ascending_sort'],
                            inplace=True)

        c2m2_df.to_csv(os.path.join(transformed_path,f'{kwargs["c2m2_entity_name"]}.tsv'),
                       sep='\t',
                       index=False)
    return wrapper

@convert_kf_to_c2m2
def convert_kf_to_project(base_df):
    base_df['abbreviation'] = base_df['SD_kf_id']
    #project_df = project_df.append(project_title_row,ignore_index=True)
    return base_df

@convert_kf_to_c2m2
def convert_kf_to_project_in_project(base_df):
    return base_df

@convert_kf_to_c2m2
def convert_kf_to_subject(kf_parts: pd.DataFrame):
    subject_df = kf_to_cfde_value_converter(kf_parts,'PT_gender')
    subject_df = kf_to_cfde_value_converter(subject_df,'PT_ethnicity')
    return subject_df


def apply_uberon_mapping(source_text, uberon_id):
    if isinstance(uberon_id,str) and uberon_id.lower().startswith('uberon'):
        return uberon_id
    elif isinstance(source_text,str):
        for anatomy_term, id in uberon_mapping_dict.items():
            if anatomy_term in source_text.lower():
                return id.upper()

#requires additional work for anatomy
@convert_kf_to_c2m2
def convert_kf_to_biosample(kf_combined_df):
    kf_combined_df['BS_uberon_id_anatomical_site'] = kf_combined_df.apply(lambda the_df: 
                                                                    apply_uberon_mapping(the_df['BS_source_text_anatomical_site'],
                                                                                         the_df['BS_uberon_id_anatomical_site']),
                                                                    axis=1)
    return kf_combined_df


def convert_days_to_years(days):
    if days:
        return f"{(days / 365):.02f}"

@convert_kf_to_c2m2
def convert_kf_to_biosample_from_subject(kf_combined_df):
    kf_combined_df['BS_age_at_event_days'] = kf_combined_df['BS_age_at_event_days'].apply(convert_days_to_years)
    return kf_combined_df
    
@convert_kf_to_c2m2
def convert_kf_to_subject_disease(kf_combined_df):
    # Ontology mapping not identified for this study
    kf_combined_df.drop(kf_combined_df.query('PT_study_id == "SD_DZ4GPQX6"').index,inplace=True)
    return kf_combined_df

@convert_kf_to_c2m2
def convert_kf_to_biosample_disease(kf_combined_df):
    # Ontology mapping not identified for this study
    kf_combined_df.drop(kf_combined_df.query('PT_study_id == "SD_DZ4GPQX6"').index,inplace=True)
    return kf_combined_df


@convert_kf_to_c2m2
def convert_kf_to_subject_role_taxonomy(kf_combined_df):
    return kf_combined_df


def get_kf_genomic_files():
    kf_combined_df = KfTableCombiner(['portal_studies',
                                      'participant',
                                      'biospecimen',
                                      'biospecimen_genomic_file',
                                      'genomic_files']) \
                    .get_combined_table()
    
    kf_combined_df.sort_values(by='GF_kf_id',inplace=True)
    return kf_combined_df

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
    hashes_df = pd.read_csv(os.path.join(ingested_path,'hashes.csv'),low_memory=False)
    # aws_scrape_df = pd.read_csv(os.path.join(ingested_path,'aws_scrape.csv'),low_memory=False)
    
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