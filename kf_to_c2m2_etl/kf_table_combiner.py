import pandas as pd
import os

from table_ops import TableJoiner

etl_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl')
ingested_path = os.path.join(etl_path,'ingested') 
transformed_path = os.path.join(etl_path,'transformed') 
conversion_path = os.path.join(etl_path,'conversion_tables') 



foreign_key_mappings = {
    'portal_studies_TO_study' : {'left':'studies_on_portal','right':'SD_kf_id'},
    'portal_studies_TO_participant' : {'left':'studies_on_portal','right':'PT_study_id'},
    'participant_TO_biospecimen' :{'left':'PT_kf_id','right':'BS_participant_id'},
    'biospecimen_TO_project_disease' :{'left':'PT_study_id','right':'study_id'},
    'participant_TO_project_disease' :{'left':'PT_study_id','right':'study_id'},
    'biospecimen_TO_biospecimen_genomic_file' :{'left':'BS_kf_id','right':'BG_biospecimen_id'},
    'biospecimen_genomic_file_TO_genomic_files' :{'left':'BG_genomic_file_id','right':'GF_kf_id'}
}

def get_keys(left_table_name, right_table_name):
    for mapping, keys in foreign_key_mappings.items():
        if left_table_name in mapping and right_table_name in mapping:
            return keys['left'], keys['right'] 

class KfTableCombiner:
    df_dict = {}
    df_dict.setdefault('portal_studies',pd.read_table(os.path.join(etl_path,'studies_on_portal.txt')))
    df_dict.setdefault('project_disease',pd.read_csv(os.path.join(conversion_path,'project_disease_matrix_only.csv')))
    df_dict.setdefault('study',pd.read_csv(os.path.join(ingested_path,'study.csv')))
    df_dict.setdefault('participant',pd.read_csv(os.path.join(ingested_path,'participant.csv')).query('visible == True'))
    df_dict.setdefault('biospecimen',pd.read_csv(os.path.join(ingested_path,'biospecimen.csv'),low_memory=False).query('visible == True'))
    df_dict.setdefault('biospecimen_genomic_file',pd.read_csv(os.path.join(ingested_path,'biospecimen_genomic_file.csv'),low_memory=False).query('visible == True'))
    # TODO: Using genomic_files is a hack. Find better solution later.
    df_dict.setdefault('genomic_files',pd.read_csv(os.path.join(ingested_path,'genomic_file.csv'),low_memory=False).query('visible == True'))

    def __init__(self, tables_to_join: list):
        self.table_list = tables_to_join

    def get_combined_table(self):
        base_df_name = self.table_list[0]
        base_df = KfTableCombiner.df_dict[base_df_name] 

        for table_name in self.table_list[1:]:
            left, right = get_keys(base_df_name,table_name)
            base_df = TableJoiner(base_df) \
                .join_kf_table(KfTableCombiner.df_dict[table_name],
                               left_key=left,
                               right_key=right) \
                .get_result()
            base_df_name = table_name
            

        return base_df

if __name__ == "__main__":
    combined = KfTableCombiner(['portal_studies','participant','biospecimen']).get_combined_table()
    print(combined.head())