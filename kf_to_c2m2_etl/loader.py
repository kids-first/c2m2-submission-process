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
                if entity['name'] in self.unpopulated_entities:
                    columns = [field['name'] for field in entity['schema']['fields']]
                    entity_df = pd.DataFrame(columns=columns)
                    entity_df.to_csv(draft_table_file_path(entity['name']), sep='\t', index=False)

def draft_table_file_path(table_name: str):
    table_name = f'{table_name}.tsv' 
    return os.path.join(file_locations.get_draft_submission_path(),table_name)
    

if __name__ == "__main__":
    loader = TsvLoader()
    loader.load_tsvs()