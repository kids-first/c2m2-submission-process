import yaml
import os

CONFIG_FILE = os.path.join(os.getcwd(),'kf_to_c2m2_etl','file_locations.yaml')

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

    def get_draft_submission_path(self):
        return self.config['paths']['draft_submission_path']

    def get_c2m2_data_package_json(self):
        return self.config['paths']['c2m2_data_package_json_path']

    def get_c2m2_table_provider_path(self):
        return self.config['paths']['c2m2_table_provider_path']

file_locations = FileLocations()