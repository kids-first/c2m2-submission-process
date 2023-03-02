import os
import pandas as pd

conversion_table_path = os.path.join(os.getcwd(),'data_ingestion','conversion_tables')

def gender_to_cfde_subject_sex(subject_df: pd.DataFrame):
    gender_df = subject_df[['gender']]

    sex_table = pd.read_csv(os.path.join(conversion_table_path,'subject_sex.tsv'),sep='\t')

    gender_df = gender_df.merge(sex_table,
                    how='left',
                    left_on='gender',
                    right_on='name')[['id']]

    gender_df.rename(columns={'id':'sex'},inplace=True)

    subject_df['gender'] = gender_df['sex']

    return subject_df

def ethnicity_to_cfde_subject_ethnicity(subject_df: pd.DataFrame):
    ethnicity_df = subject_df[['ethnicity']]
    ethnicity_df['ethnicity'] = ethnicity_df['ethnicity'].apply(normalize_string)

    ethnicity_table = pd.read_csv(os.path.join(conversion_table_path,'subject_ethnicity.tsv'),sep='\t')
    ethnicity_table['name'] = ethnicity_table['name'].apply(normalize_string)

    ethnicity_df = ethnicity_df.merge(ethnicity_table,
                    how='left',
                    left_on='ethnicity',
                    right_on='name')[['id']]

    ethnicity_df.rename(columns={'id':'ethnicity'},inplace=True)

    subject_df['ethnicity'] = ethnicity_df['ethnicity']

    return subject_df

def normalize_string(value: str):
    if isinstance(value,str):
        return value.lower()
    return "" 