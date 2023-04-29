import os
import pandas as pd
from typing import List

from pandas_io_util import PandasCsvUpdater, handle_pre_existing_files
from time_keeper import Timer

from ingest import Ingest

targets = ['participants','diagnoses','studies',
             'biospecimens','biospecimen-diagnoses',
             'genomic-files','biospecimen-genomic-files']

project_directory = os.getcwd()
etl_path = os.path.join(os.getcwd(),'kf_to_c2m2_etl')
test_ingest_dir = os.path.join(project_directory,'kf_to_c2m2_etl','ingest_test')

timer = Timer()

def main():
    studies = pd.read_table(os.path.join(etl_path,'studies_on_portal.tsv'))['studies_on_portal'].to_list()

    ingestor = Ingest(studies)

    ingestor.get_file_metadata()


    the_study = ingestor.extract()

    for index, (study, endpoints) in enumerate(the_study.items()):
        for endpoint, the_df in endpoints.items():
            if isinstance(the_df,pd.DataFrame) and endpoint in targets:
                the_df.sort_values(by=['kf_id'],inplace=True)
                PandasCsvUpdater(endpoint,the_df).update_csv_with_df()


def prepare_test_ingest_directory():
    try:
        os.mkdir(test_ingest_dir)
    except:
        print('Transformed directory already exists.... Skipping directory creation.') 

if __name__ == "__main__":
    prepare_test_ingest_directory()
    handle_pre_existing_files()
    main()