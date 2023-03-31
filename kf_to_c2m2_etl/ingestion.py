import time
import warnings
import os
import pandas as pd
from typing import List
from kf_utils.dataservice.scrape import *
from transform import is_tsv

endpoints = ['participants','diagnoses','studies',
             'biospecimens','biospecimen-diagnoses',
             'genomic-files','biospecimen-genomic-files']

kf_ds_url_base = 'https://kf-api-dataservice.kidsfirstdrc.org'

def main():
    required_endpoints = set(endpoints) - set(pre_existing_tables())

    start = time.time()

    write_all_studies_to_tsv_s(required_endpoints)
    
    end = time.time()
    time_elapsed_mins = (end - start) / 60

    print(f'time elapsed acquiring datasets: {time_elapsed_mins}')


def get_biospecimen_from_participants():
    biospecimen_df : pd.DataFrame = None
    for entity in yield_entities(kf_ds_url_base,'biospecimens',{},show_progress=True):
        entity = remove_collection_values(entity)
        if biospecimen_df is None:
            biospecimen_df = pd.DataFrame.from_dict(entity,orient='index').T
        else:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                biospecimen_df = biospecimen_df.append(pd.DataFrame.from_dict(entity,orient='index').T,ignore_index=True)

    biospecimen_df.to_csv(f'data_ingestion/ingested/biospecimens.tsv',sep='\t',index=None)


def get_visible_studies() -> List[str]:
    with open('data_ingestion/studies_on_portal.txt','r') as studies_file:
        return [study.strip() for study in studies_file]


def get_kf_participants():
    kf_participants_df : pd.DataFrame = None
    for study in get_visible_studies():
        for entity in yield_entities(kf_ds_url_base,'participants',{'study_id':study},show_progress=True):
            entity = remove_collection_values(entity)
            if kf_participants_df is None:
                kf_participants_df = pd.DataFrame.from_dict(entity,orient='index').T
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    kf_participants_df = kf_participants_df.append(pd.DataFrame.from_dict(entity,orient='index').T,ignore_index=True)

    kf_participants_df.to_csv(f'data_ingestion/ingested/participants.tsv',sep='\t',index=None)


def write_all_studies_to_tsv_s(required_endpoints):
    for endpoint in required_endpoints:
        dataset_df : pd.DataFrame = None
        for entity in yield_entities(kf_ds_url_base,endpoint,{},show_progress=True):
            entity = remove_collection_values(entity)
            if dataset_df is None:
                dataset_df = pd.DataFrame.from_dict(entity,orient='index').T
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    dataset_df = dataset_df.append(pd.DataFrame.from_dict(entity,orient='index').T,ignore_index=True)
        dataset_df.to_csv(f'data_ingestion/ingested/{endpoint}.tsv',sep='\t',index=None)
    print(f'completed endpoint : {endpoint}')


def remove_collection_values(entity: dict):
    new_dict = {}
    for key, value in entity.items(): 
        if not (isinstance(value,dict) or isinstance(value,list)):
            new_dict.setdefault(key,value)
    return new_dict


def pre_existing_tables():
    data_path = os.path.join(os.getcwd(),'data_ingestion/ingested')

    tsv_s = None
    with os.scandir(data_path) as directory:
        tsv_s = [tsv.name.split('.')[0] for tsv in filter(is_tsv,directory)]

    return tsv_s 


def write_json_data_to_tsv(dataset_name: str, dataset_jsons: list):
    prepare_ingestion_directory()
    result_df: pd.DataFrame 
    for index, result_jsons in enumerate(dataset_jsons):
        if index == 0:
            result_df = get_df_from_json(result_jsons)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                result_df = result_df.append(get_df_from_json(result_jsons),ignore_index=True)
    
    result_df.drop_duplicates()
    result_df.to_csv(f'data_ingestion/ingested/{dataset_name}.tsv',sep='\t',index=None)


def get_df_from_json(json) -> pd.DataFrame:
    results = json.get('results')

    for result in results:
        if result.get('_links').get('study'):
            result.update({'study_id' : result.get('_links').get('study').split('/')[-1]})
        del result['_links']
    
    table_columns = [col for col in results[0] if col != '_links']
    df = pd.DataFrame(results,columns=table_columns)

    return df


def prepare_ingestion_directory():
    ingestd_path = os.path.join(os.getcwd(),'data_ingestion','ingested')
    try:
        os.mkdir(ingestd_path)
    except:
        print('Ingested directory already exists.... Skipping directory creation.')


if __name__ == "__main__":
    main()