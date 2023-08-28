import os
import pandas as pd
import shutil

from pandas_io_util import handle_pre_existing_files

from etl_types import ETLType
from ingest import Ingest, write_studies_to_disk 
from fhir_ingest import FhirIngest, write_fhir_studies_to_disk
from transform import transform_kf_to_c2m2_on_disk
from fhir_transform import transform_fhir_to_c2m2_on_disk
from loader import TsvLoader
from file_locations import file_locations

portal_studies_path = os.path.join(file_locations.get_etl_path(),'studies_on_portal.tsv')
kf_studies_on_fhir = ['SD_JWS3V24D','SD_FFVQ3T38','SD_DYPMEHHF']

def main():

    ingest_type = get_etl_type_from_user() 

    if ingest_type == ETLType.DS:
        studies = pd.read_table(portal_studies_path)['studies_on_portal'].to_list()

        dataservice_ingestor = Ingest(studies)

        dataservice_ingestor.get_file_metadata()

        write_studies_to_disk(dataservice_ingestor.extract())

        transform_kf_to_c2m2_on_disk()

    elif ingest_type == ETLType.FHIR:
        fhir_ingestor = FhirIngest(['SD_65064P2Z','SD_DYPMEHHF','SD_FYCR78W0','SD_Z6MWD3H0'])

        write_fhir_studies_to_disk(fhir_ingestor.extract())

        transform_fhir_to_c2m2_on_disk()

    
    TsvLoader().load_tsvs()


def prepare_etl_directories():
    directories = [
        file_locations.get_ingested_path(),
        file_locations.get_transformed_path(),
        file_locations.get_draft_submission_path(),
        file_locations.get_auto_gen_path()
    ]
    
    for directory in directories:
        if os.path.isdir(directory):
            print(f"Clearing directory '{directory}'")
            shutil.rmtree(directory)
            
        try:
            os.mkdir(directory)
        except FileExistsError:
            print(f"Directory '{directory}' already exists. Skipping directory creation.") 


def get_etl_type_from_user():
    while True:
        user_input = input("Would you like to do an etl from FHIR or DS? ").upper()

        if user_input == 'FHIR':
            return ETLType.FHIR
        elif user_input == 'DS':
            return ETLType.DS
        else:
            print("Invalid input. Please enter 'FHIR' or 'DS'.")


if __name__ == "__main__":
    prepare_etl_directories()
    handle_pre_existing_files()
    main()