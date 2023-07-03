import os
import pandas as pd
from typing import List
import logging 

from cfde_convert import kf_to_cfde_value_converter
from table_ops import TableJoiner, reshape_kf_combined_to_c2m2
from file_locations import file_locations
from kf_table_combiner import KfTableCombiner
from etl_types import ETLType

from value_converter import modify_dbgap, get_persistent_id, \
                            path_to_filename, convert_days_to_years, \
                            apply_uberon_mapping

logging.basicConfig(level=logging.INFO)

def transform_kf_to_c2m2_on_disk():
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


    convert_kf_to_file(kf_combined_list=['portal_studies','participant','biospecimen',
                                         'biospecimen_genomic_file','genomic_files'],
                                          c2m2_entity_name='file',
                                          sort_on='local_id',
                                          ascending_sort=True)

    convert_kf_to_file_describes_biosample(kf_combined_list=['portal_studies','participant','biospecimen','biospecimen_genomic_file','genomic_files'],
                                          c2m2_entity_name='file_describes_biosample',
                                          sort_on=['biosample_local_id','file_local_id'],
                                          ascending_sort=True)


def convert_kf_to_c2m2(func):
    def wrapper(**kwargs):
        kf_combined_df = KfTableCombiner(kwargs['kf_combined_list']).get_combined_table()
        combined_adjusted = func(kf_combined_df)
        c2m2_df = reshape_kf_combined_to_c2m2(combined_adjusted,kwargs['c2m2_entity_name'])
        
        c2m2_df.sort_values(by=kwargs['sort_on'],
                            ascending=kwargs['ascending_sort'],
                            inplace=True)

        c2m2_df.drop_duplicates(inplace=True)
        c2m2_df.to_csv(os.path.join(file_locations.get_transformed_path(),f'{kwargs["c2m2_entity_name"]}.tsv'),
                       sep='\t',
                       index=False)
    return wrapper

@convert_kf_to_c2m2
def convert_kf_to_project(base_df):
    logging.info('Converting kf to c2m2 project')
    base_df['abbreviation'] = base_df['SD_kf_id']
    return base_df

@convert_kf_to_c2m2
def convert_kf_to_project_in_project(base_df):
    logging.info('Converting kf to c2m2 project in project')
    return base_df

@convert_kf_to_c2m2
def convert_kf_to_subject(kf_parts: pd.DataFrame):
    logging.info('Converting kf to c2m2 subject')
    subject_df = kf_to_cfde_value_converter(ETLType.DS, kf_parts,'PT_gender')
    subject_df = kf_to_cfde_value_converter(ETLType.DS, subject_df,'PT_ethnicity')
    return subject_df



#requires additional work for anatomy
@convert_kf_to_c2m2
def convert_kf_to_biosample(kf_combined_df):
    logging.info('Converting kf to c2m2 biosample')
    kf_combined_df['BS_uberon_id_anatomical_site'] = kf_combined_df.apply(lambda the_df: 
                                                                    apply_uberon_mapping(ETLType.DS,
                                                                                         the_df['BS_source_text_anatomical_site'],
                                                                                         the_df['BS_uberon_id_anatomical_site']),
                                                                    axis=1)
    return kf_combined_df


@convert_kf_to_c2m2
def convert_kf_to_biosample_from_subject(kf_combined_df):
    logging.info('Converting kf to c2m2 biosample from subject')
    kf_combined_df['BS_age_at_event_days'] = kf_combined_df['BS_age_at_event_days'].apply(convert_days_to_years)
    return kf_combined_df
    
@convert_kf_to_c2m2
def convert_kf_to_subject_disease(kf_combined_df):
    logging.info('Converting kf to c2m2 subject disease')
    # Ontology mapping not identified for this study
    kf_combined_df.drop(kf_combined_df.query('PT_study_id == "SD_DZ4GPQX6"').index,inplace=True)
    return kf_combined_df

@convert_kf_to_c2m2
def convert_kf_to_biosample_disease(kf_combined_df):
    logging.info('Converting kf to c2m2 biosample disease')
    # Ontology mapping not identified for this study
    kf_combined_df.drop(kf_combined_df.query('PT_study_id == "SD_DZ4GPQX6"').index,inplace=True)
    return kf_combined_df


@convert_kf_to_c2m2
def convert_kf_to_subject_role_taxonomy(kf_combined_df):
    logging.info('Converting kf to c2m2 subject role taxomonmy')
    return kf_combined_df


@convert_kf_to_c2m2
def convert_kf_to_file(kf_genomic_files):
    logging.info('Converting kf to c2m2 file')

    seq_exp_gen_files_df = pd.read_csv(os.path.join(file_locations.get_ingested_path(),'sequencing-experiment-genomic-files.csv'),
                                       low_memory=False) \
                                        .query('visible == True')

    seq_exp_df = pd.read_csv(os.path.join(file_locations.get_ingested_path(),'sequencing-experiments.csv'),
                                       low_memory=False) \
                                        .query('visible == True')

    indexd_df = pd.read_csv(os.path.join(file_locations.get_ingested_path(),'indexd_scrape.csv'),low_memory=False)
    hashes_df = pd.read_csv(os.path.join(file_locations.get_ingested_path(),'hashes.csv'),low_memory=False)
    
    metadata_df = TableJoiner(indexd_df) \
            .join_kf_table(hashes_df,
                           left_key='url',
                           right_key='s3path') \
            .get_result()                   

    kf_genomic_files = TableJoiner(kf_genomic_files) \
                    .left_join(metadata_df,
                               left_key='GF_latest_did',
                               right_key='did') \
                    .get_result()

    experiment_strategy = TableJoiner(seq_exp_gen_files_df) \
                    .join_kf_table(seq_exp_df,
                               left_key='SG_sequencing_experiment_id',
                               right_key='SE_kf_id',) \
                    .get_result()

    kf_genomic_files = TableJoiner(kf_genomic_files) \
                    .left_join(experiment_strategy,
                               left_key='GF_kf_id',
                               right_key='SG_genomic_file_id') \
                    .get_result()

    file_df = kf_to_cfde_value_converter(ETLType.DS, kf_genomic_files,'GF_file_format')
    file_df = kf_to_cfde_value_converter(ETLType.DS, file_df,'GF_data_type')
    file_df = kf_to_cfde_value_converter(file_df,'SE_experiment_strategy')
    
    file_df['BS_dbgap_consent_code'] = file_df['BS_dbgap_consent_code'].apply(modify_dbgap)
    file_df['persistent_id'] = file_df.apply(lambda the_df: 
                                             get_persistent_id(the_df['PT_study_id'],
                                                               the_df['GF_latest_did'],
                                                               the_df['md5']),
                                             axis=1)

    file_df['filename'] = file_df['GF_external_id'].apply(path_to_filename)

    return file_df

@convert_kf_to_c2m2
def convert_kf_to_file_describes_biosample(kf_genomic_files: pd.DataFrame):
    logging.info('Converting kf to c2m2 file describes biosample')
    indexd_df = pd.read_csv(os.path.join(file_locations.get_ingested_path(),'indexd_scrape.csv'),low_memory=False)
    hashes_df = pd.read_csv(os.path.join(file_locations.get_ingested_path(),'hashes.csv'),low_memory=False)

    metadata_df = TableJoiner(indexd_df) \
            .join_kf_table(hashes_df,
                           left_key='url',
                           right_key='s3path') \
            .get_result()                   

    kf_genomic_files = TableJoiner(kf_genomic_files) \
                    .left_join(metadata_df,
                               left_key='GF_latest_did',
                               right_key='did') \
                    .get_result()

    return kf_genomic_files

if __name__ == "__main__":
    transform_kf_to_c2m2_on_disk()