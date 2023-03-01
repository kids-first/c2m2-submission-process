import os
import pandas as pd
from typing import List

ingestion_path = os.path.join(os.getcwd(),'data_ingestion')
ingested_path = os.path.join(ingestion_path,'ingested') 
transformed_path = os.path.join(ingestion_path,'transformed') 

def main():
    prepare_transformed_directory()

    kf_participants = get_kf_visible_participants()

    # get_kf_biospecimens(kf_parts=kf_participants)

    get_biosample_from_subject(kf_parts=kf_participants)

def get_kf_visible_participants():
    kf_participant_df = pd.read_csv(os.path.join(ingested_path,'participant.csv'))
    studies_df = pd.read_table(os.path.join(ingestion_path,'studies_on_portal.txt'))

    visible_pariticipants = kf_participant_df.merge(studies_df,
                                                    how='inner',
                                                    left_on='study_id',
                                                    right_on='studies_on_portal')

    visible_pariticipants = visible_pariticipants[['kf_id','study_id','gender','ethnicity']]
    visible_pariticipants.to_csv(os.path.join(transformed_path,'subjects.tsv'),sep='\t',index=False)

    return visible_pariticipants


def get_kf_biospecimens(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)

    kf_biospec_df = biospec_df.merge(kf_parts,
                                     how='inner',
                                     left_on='participant_id',
                                     right_on='kf_id')

    kf_biospec_df = kf_biospec_df[['kf_id_x','study_id','created_at','uberon_id_anatomical_site']]
    kf_biospec_df.sort_values(by=['kf_id_x'],ascending=True,inplace=True)
    kf_biospec_df.to_csv(os.path.join(transformed_path,'biospecimens.tsv'),sep='\t',index=False)


def get_biosample_from_subject(kf_parts: pd.DataFrame) -> None:
    biospec_df = pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False)

    kf_biospec_df = biospec_df.merge(kf_parts,
                                     how='inner',
                                     left_on='participant_id',
                                     right_on='kf_id')

    kf_biospec_df['biosample_id_namespace'] = 'kidsfirst:'
    kf_biospec_df['subject_id_namespace'] = 'kidsfirst:'
    kf_biospec_df['age_at_event_days'] =  kf_biospec_df['age_at_event_days'] / 365


    kf_biospec_df = kf_biospec_df[['biosample_id_namespace','kf_id_x','subject_id_namespace','kf_id_y','age_at_event_days']]

    kf_biospec_df.rename(columns={'kf_id_x':'biosample_local_id',
                                  'kf_id_y':'subject_local_id',
                                  'age_at_event_days':'age_at_sampling'},
                                  inplace=True)

    kf_biospec_df.to_csv(os.path.join(transformed_path,'biosample_from_subject.tsv'),sep='\t',index=False)
    

def is_tsv(file : os.DirEntry):
    return file.is_file() and file.name.endswith('.tsv')

def prepare_transformed_directory():
    try:
        os.mkdir(transformed_path)
    except:
        print('Transformed directory already exists.... Skipping directory creation.')


if __name__ == "__main__":
    main()