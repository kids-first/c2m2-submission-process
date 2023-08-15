import os
import pandas as pd
import shutil

from pandas_io_util import handle_pre_existing_files

from ingest import Ingest, write_studies_to_disk, IngestType
from fhir_ingest import FhirIngest
from transform import transform_kf_to_c2m2_on_disk
from loader import TsvLoader
from file_locations import file_locations

portal_studies_path = os.path.join(file_locations.get_etl_path(),'studies_on_portal.tsv')
kf_studies_on_fhir = ['SD_JWS3V24D','SD_FFVQ3T38','SD_DYPMEHHF']

def main():

    # studies = pd.read_table(portal_studies_path)['studies_on_portal'].to_list()


    fhir_ingestor = FhirIngest()

    dataservice_ingestor = Ingest(fhir_ingestor.studies['studies'].to_list())

    dataservice_ingestor.get_file_metadata()

    write_studies_to_disk(IngestType.DS, dataservice_ingestor.extract())

    write_studies_to_disk(IngestType.FHIR, fhir_ingestor.extract())

    transform_kf_to_c2m2_on_disk()

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


if __name__ == "__main__":
    prepare_etl_directories()
    handle_pre_existing_files()
    main()