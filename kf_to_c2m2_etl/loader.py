import os
import shutil
import json
import pandas as pd

from file_locations import file_locations

class TsvLoader:
    def __init__(self): 
        self.tsv_dir = file_locations.get_transformed_path()
        self.dp_path = file_locations.get_c2m2_data_package_json()
        self.draft_dir = file_locations.get_draft_submission_path()
        self.loaded_files = set()

    def load_tsvs(self):
        self._load_tsvs_to_draft()
        self._get_missing_c2m2_entities()
        self._load_tables_for_missing_c2m2_entities()


    def _load_tsvs_to_draft(self):

        os.makedirs(self.draft_dir, exist_ok=True)

        get_dcc_df().to_csv(os.path.join(self.tsv_dir,'dcc.tsv'),sep='\t',index=False)
        get_id_namespace().to_csv(os.path.join(self.tsv_dir,'id_namespace.tsv'),sep='\t',index=False)

        # iterate over the TSV files in the directory
        for filename in os.listdir(self.tsv_dir):
            if isinstance(filename,str) and filename.endswith('.tsv'):
                # copy the TSV file to the draft submission directory
                shutil.copy(os.path.join(self.tsv_dir, filename), self.draft_dir)
                self.loaded_files.add(filename.split('.')[0])
        

    def _get_missing_c2m2_entities(self):
        with open(self.dp_path) as c2m2_json_file:
            c2m2_description_package = json.load(c2m2_json_file)

            all_entities = set(resource['name'] for resource in c2m2_description_package['resources'])
            self.unpopulated_entities = all_entities - self.loaded_files
    
    def _load_tables_for_missing_c2m2_entities(self):
        with open(self.dp_path) as c2m2_json_file:
            c2m2_description_package = json.load(c2m2_json_file)

            for entity in c2m2_description_package['resources']: 
                if entity['name'] in self.unpopulated_entities and is_prepared_by_submitter(entity['path']):
                    columns = [field['name'] for field in entity['schema']['fields']]
                    entity_df = pd.DataFrame(columns=columns)
                    entity_df.to_csv(draft_table_file_path(entity['name']), sep='\t', index=False)

def draft_table_file_path(table_name: str):
    table_name = f'{table_name}.tsv' 
    return os.path.join(file_locations.get_draft_submission_path(),table_name)

def get_c2m2_table_origin_dict():
    table_provider_df = pd.read_csv(file_locations.get_c2m2_table_provider_path(),sep='\t')
    return dict(zip(table_provider_df['Table'],table_provider_df['Construction']))

def is_prepared_by_submitter(table_name: str):
    try:
        return get_c2m2_table_origin_dict()[table_name] == 'Prepared by submitter'
    except KeyError:
        print(f'Table {table_name} is not a valid C2M2 table.')
        return False
        

def get_dcc_df():
    return pd.DataFrame([
        {'id':'cfde_registry_dcc:kidsfirst',
         'dcc_name':'The Gabriella Miller Kids First Pediatric Research Program',
         'dcc_abbreviation':'KFDRC',
         'dcc_description':'A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.',
         'contact_email':'support@kidsfirstdrc.org',
         'contact_name':'Kids First Support',
         'dcc_url':'https://kidsfirstdrc.org',
         'project_id_namespace':'kidsfirst:',
         'project_local_id':'drc',
         }
    ])


def get_id_namespace():
    return pd.DataFrame([
        {'id':'kidsfirst:',
         'abbreviation':'KFDRC_NS',
         'name':'The Gabriella Miller Kids First Pediatric Research Program',
         'description':'A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.',
         }
    ])


if __name__ == "__main__":
    loader = TsvLoader()
    loader.load_tsvs()
    