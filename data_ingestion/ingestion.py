import requests
import pandas as pd
import time

def get_df_from_json(json):
    results = json.get('results')

    for result in results:
        result.update({'study_id' : result.get('_links').get('study').split('/')[-1]})
        del result['_links']
    
    df = pd.DataFrame(results,columns=['kf_id','study_id','created_at','gender','ethnicity','program'])

    return df


resp = requests.get('https://kf-api-dataservice.kidsfirstdrc.org/participants',
                        headers={'Content-Type': 'application/json'})

df = get_df_from_json(resp.json())

record_count = 10

start = time.time()
while(next_part := resp.json().get("_links").get('next')):
    record_count+=10
    print('record_count: ' + str(record_count))

    resp = requests.get('https://kf-api-dataservice.kidsfirstdrc.org' + next_part,
                        headers={'Content-Type': 'application/json'})
    df = df.append(get_df_from_json(resp.json()),ignore_index=True)

end = time.time()

df.to_csv('data_ingestion/subject.tsv',sep='\t',index=None)
print('time elapsed: ' + str((end-start)/60) + ' mins')
print('record_count: ' + str(record_count))

