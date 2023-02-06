import requests
import pandas as pd
import time
import warnings


def main():
    result_dict = {'participant': get_full_dataset_from_endpoint('https://kf-api-dataservice.kidsfirstdrc.org/participants'),
                   'study': get_full_dataset_from_endpoint('https://kf-api-dataservice.kidsfirstdrc.org/studies')}
    
    
    result_dfs = {}
    for title, dataset in result_dict.items():
        for index, result_jsons in enumerate(dataset):
            if index == 0:
                result_dfs.setdefault(title, get_df_from_json(result_jsons))
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    temp_df = get_df_from_json(result_jsons)
                    result_dfs.update({title: result_dfs.get(title).append(temp_df,ignore_index=True)})

    for df_title in result_dict.keys():
        result_dfs.get(df_title).drop_duplicates()
        result_dfs.get(df_title).to_csv(f'data_ingestion/{df_title}.tsv',sep='\t',index=None)


def get_df_from_json(json) -> pd.DataFrame:
    results = json.get('results')

    for result in results:
        if result.get('_links').get('study'):
            result.update({'study_id' : result.get('_links').get('study').split('/')[-1]})
        del result['_links']
    
    table_columns = [col for col in results[0] if col != '_links']
    df = pd.DataFrame(results,columns=table_columns)

    return df

def get_full_dataset_from_endpoint(url : str) -> list:
    result_list = []
    limit_query = 'limit=100'

    resp = requests.get(url + '?' + limit_query,
                        headers={'Content-Type': 'application/json'})
    result_list.append(resp.json())

    record_count = len(resp.json().get('results'))

    start = time.time()
    while((next_part := resp.json().get("_links").get('next'))):

        next_participant_removed_uuid = next_part.split('&')[0]
        limit = 'limit=100'

        resp = requests.get('https://kf-api-dataservice.kidsfirstdrc.org' + next_participant_removed_uuid + '&' + limit,
                            headers={'Content-Type': 'application/json'})
        result_list.append(resp.json())

        record_count += len(resp.json().get('results'))
        print('record_count: ' + str(record_count))

    end = time.time()
    print('time elapsed: ' + str((end-start)/60) + ' mins')
    print('record_count: ' + str(record_count))

    return result_list 


if __name__ == "__main__":
    main()