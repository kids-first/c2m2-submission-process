import yaml
import os

ETL_DIR = 'kf_to_c2m2_etl'
if ETL_DIR not in os.getcwd():
    CONFIG_FILE = os.path.join(os.getcwd(),'kf_to_c2m2_etl','file_locations.yaml')
else:
    CONFIG_FILE = os.path.join(os.getcwd(),'file_locations.yaml')
class FileLocations:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            with open(CONFIG_FILE, 'r') as f:
                cls._instance.config = yaml.safe_load(f)
        return cls._instance

    def get_etl_path(self):
        return self.config['paths']['etl_path']

    def get_ingested_path(self):
        return self.config['paths']['ingested_path']

    def get_transformed_path(self):
        return self.config['paths']['transformed_path']

    def get_conversion_path(self):
        return self.config['paths']['conversion_path']

    def get_cfde_reference_table_path(self):
        return self.config['paths']['cfde_ref_table_path']

    def get_kf_to_c2m2_mappings_path(self):
        return self.config['paths']['kf_to_c2m2_mappings_path']

    def get_ontology_mappings_path(self):
        return self.config['paths']['ontology_mappings_path']

    def get_draft_submission_path(self):
        return self.config['paths']['draft_submission_path']

    def get_c2m2_data_package_json(self):
        return self.config['paths']['c2m2_data_package_json_path']

    def get_c2m2_table_provider_path(self):
        return self.config['paths']['c2m2_table_provider_path']

    def get_fhir_mapping_paths(self):
        return self.config['paths']['fhir_mapping_path']

    def get_auto_gen_path(self):
        return self.config['paths']['auto_gen_path']

file_locations = FileLocations()
print(f'etl path: {file_locations.get_etl_path()}')