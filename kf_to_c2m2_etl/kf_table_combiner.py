import pandas as pd
import os

from table_ops import TableJoiner, is_column_present
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
        'genomic_files': {'left': 'BG_genomic_file_id', 'right': 'GF_kf_id'},
        'sequencing_experiment_genomic_file': {'left': '', 'right': ''}
    },
    'genomic_files': {
        'sequencing_experiment_genomic_file': {'left':'GF_kf_id', 'right':'SG_genomic_file_id'}
    },
    'sequencing_experiment_genomic_file': {
        'sequencing_experiment': {'left': 'SG_sequencing_experiment_id', 'right': 'SE_kf_id'}
    }
}

kf_tablenames = ['study','participant','biospecimen','biospecimen_genomic_file',
                # TODO: Using genomic_files is a hack. Find better solution later.
                 'genomic_files','sequencing_experiment_genomic_file','sequencing_experiment']

kf_tables_with_visibility = ['participant','biospecimen','biospecimen_genomic_file',
                             'genomic_files','sequencing_experiment_genomic_file','sequencing_experiment']

# Required due to ingest using endpoint names when ingesting tables
table_to_endpoint_name = {
    'study': 'studies',
    'participant': 'participants',
    'biospecimen': 'biospecimens',
    'biospecimen_genomic_file': 'biospecimen-genomic-files',
    'genomic_files': 'genomic-files',
    'sequencing_experiment_genomic_file': 'sequencing-experiment-genomic-files',
    'sequencing_experiment': 'sequencing-experiments'
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
    df_dict.setdefault('project_disease',pd.read_table(os.path.join(file_locations.get_ontology_mappings_path(),'project_disease_matrix_only.tsv')))

    def __init__(self, tables_to_join: list):
        """
        Initializes the KfTableCombiner object.

        Args:
            tables_to_join (list): A list of tables to be joined.
        """
        self.table_list = tables_to_join
        self.add_tables_to_df_dict()

    def add_tables_to_df_dict(self):
        """
        Dynamically adds tables to the `df_dict` dictionary.
        """
        for table_name in self.table_list:
            if table_name not in KfTableCombiner.df_dict:
                file_path = os.path.join(file_locations.get_ingested_path(),f'{table_to_endpoint_name[table_name]}.csv')

                if table_name in kf_tablenames:
                    if table_name in kf_tables_with_visibility and is_column_present(file_path, 'visible'):
                        table_df = pd.read_csv(file_path, low_memory=False).query('visible == True')
                    else:
                        table_df = pd.read_csv(file_path, low_memory=False)

                    KfTableCombiner.df_dict[table_name] = table_df

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