import requests
import time
import warnings
import os
import pandas as pd
from transform import is_tsv

endpoints = ['participants','diagnoses','studies',
             'biospecimens','biospecimen-diagnoses',
             'genomic-files','biospecimen-genomic-files']

def main():
    required_endpoints = set(endpoints) - set(pre_existing_tables())

    kf_ds_url_base = 'https://kf-api-dataservice.kidsfirstdrc.org'

    start = time.time()

    [get_full_dataset_from_endpoint(os.path.join(kf_ds_url_base,endpoint)) for endpoint in required_endpoints]

    end = time.time()
    time_elapsed_mins = (end - start) / 60

    print(f'time elapsed acquiring datasets: {time_elapsed_mins}')


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

def get_full_dataset_from_endpoint(url : str) -> None:
    result_list = []
    limit_query = 'limit=100'

    resp = requests.get(url + '?' + limit_query,
                        headers={'Content-Type': 'application/json'})
    result_list.append(resp.json())

    record_count = len(resp.json().get('results'))

    while((next_part := resp.json().get("_links").get('next'))):

        next_participant_removed_uuid = next_part.split('&')[0]
        limit = 'limit=100'

        resp = requests.get('https://kf-api-dataservice.kidsfirstdrc.org' + next_participant_removed_uuid + '&' + limit,
                            headers={'Content-Type': 'application/json'})
        result_list.append(resp.json())

        record_count += len(resp.json().get('results'))
        if record_count % 1000 == 0:
            print(f'current record count = {record_count}')

    dataset_name = url.split('/')[-1]
    print(f'completed record_count = {record_count} for:  {dataset_name}')
    write_json_data_to_tsv(dataset_name,result_list)


def prepare_ingestion_directory():
    ingestd_path = os.path.join(os.getcwd(),'data_ingestion','ingested')
    try:
        os.mkdir(ingestd_path)
    except:
        print('Ingested directory already exists.... Skipping directory creation.')


if __name__ == "__main__":
    main()