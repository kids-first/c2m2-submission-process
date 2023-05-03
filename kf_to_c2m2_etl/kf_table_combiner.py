import pandas as pd
import os

from table_ops import TableJoiner
from file_locations import file_locations


foreign_key_mappings = {
    'portal_studies': {
        'study': {'left': 'studies_on_portal', 'right': 'SD_kf_id'},
        'participant': {'left': 'studies_on_portal', 'right': 'PT_study_id'}
    },
    'participant': {
        'biospecimen': {'left': 'PT_kf_id', 'right': 'BS_participant_id'},
        'project_disease': {'left': 'PT_study_id', 'right': 'study_id'}
    },
    'biospecimen': {
        'project_disease': {'left': 'PT_study_id', 'right': 'study_id'},
        'biospecimen_genomic_file': {'left': 'BS_kf_id', 'right': 'BG_biospecimen_id'}
    },
    'biospecimen_genomic_file': {
        'genomic_files': {'left': 'BG_genomic_file_id', 'right': 'GF_kf_id'}
    }
}


class KfTableCombiner:
    """
    A class for combining tables from the Kids First platform.

    Attributes:
        df_dict (dict): A dictionary containing data frames for several tables.
        table_list (list): A list of tables to be joined.

    Methods:
        get_keys: Returns the keys to be used in joining two tables.
        get_combined_table: Returns a data frame containing the combined table.
    """
    df_dict = {}
    df_dict.setdefault('portal_studies',pd.read_table(os.path.join(file_locations.get_etl_path(),'studies_on_portal.tsv')))
    df_dict.setdefault('project_disease',pd.read_table(os.path.join(file_locations.get_conversion_path(),'project_disease_matrix_only.tsv')))
    df_dict.setdefault('study',pd.read_csv(os.path.join(file_locations.get_ingested_path(),'studies.csv')))
    df_dict.setdefault('participant',pd.read_csv(os.path.join(file_locations.get_ingested_path(),'participants.csv')).query('visible == True'))
    df_dict.setdefault('biospecimen',pd.read_csv(os.path.join(file_locations.get_ingested_path(),'biospecimens.csv'),low_memory=False).query('visible == True'))
    df_dict.setdefault('biospecimen_genomic_file',pd.read_csv(os.path.join(file_locations.get_ingested_path(),'biospecimen-genomic-files.csv'),low_memory=False).query('visible == True'))
    # TODO: Using genomic_files is a hack. Find better solution later.
    df_dict.setdefault('genomic_files',pd.read_csv(os.path.join(file_locations.get_ingested_path(),'genomic-files.csv'),low_memory=False).query('visible == True'))

    def __init__(self, tables_to_join: list):
        """
        Initializes the KfTableCombiner object.

        Args:
            tables_to_join (list): A list of tables to be joined.
        """
        self.table_list = tables_to_join

    def get_combined_table(self) -> pd.DataFrame:
        """
        Returns a data frame containing the combined table.

        Returns:
            A data frame containing the combined table.
        """
        base_df_name = self.table_list[0]
        base_df = KfTableCombiner.df_dict[base_df_name] 

        for table_name in self.table_list[1:]:
            left, right = foreign_key_mappings[base_df_name][table_name].values() 
            base_df = TableJoiner(base_df) \
                .join_kf_table(KfTableCombiner.df_dict[table_name],
                               left_key=left,
                               right_key=right) \
                .get_result()
            base_df_name = table_name
            

        return base_df