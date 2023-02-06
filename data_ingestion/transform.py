import os
import pandas as pd


def main():
    data_path = os.path.join(os.getcwd(),'data_ingestion')

    df_dict = {}
    with os.scandir(data_path) as directory:
        tsv_s = filter(is_tsv,directory)
        for tsv in tsv_s:
            df_dict.setdefault(tsv.name, pd.read_table(tsv))
    
    subjects_df = join_participant_study_tables(df_dict)

    subjects_df.to_csv(f'data_ingestion/subjects.tsv',sep='\t',index=None)


def join_participant_study_tables(df_dict: dict):
    part_df = df_dict.get('participant.tsv')
    study_df = df_dict.get('study.tsv')
    kf_studies = study_df.query('program == "Kids First"')
    

    return pd.merge(part_df[['kf_id','study_id','created_at','gender','ethnicity']], kf_studies[['kf_id','program']], left_on='study_id',right_on='kf_id',how='inner')

def is_tsv(file : os.DirEntry):
    return file.is_file() and file.name.endswith('.tsv')


if __name__ == "__main__":
    main()