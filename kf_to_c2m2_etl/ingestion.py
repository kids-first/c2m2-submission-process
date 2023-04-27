import os
import pandas as pd
from typing import List
from kf_table_combiner import KfTableCombiner
from pandas_io_util import PandasCsvUpdater

from ingest import Ingest

targets = ['participants','diagnoses','studies',
             'biospecimens','biospecimen-diagnoses',
             'genomic-files','biospecimen-genomic-files']

project_directory = os.getcwd()
test_ingest_dir = os.path.join(project_directory,'kf_to_c2m2_etl','ingest_test')

def main():
    studies = KfTableCombiner(['portal_studies'])\
            .get_combined_table()['studies_on_portal'].to_list()

    ingestor = Ingest(studies[:2])

    the_study = ingestor.extract()

    for index, (study, endpoints) in enumerate(the_study.items()):
        for endpoint, the_df in endpoints.items():
            if isinstance(the_df,pd.DataFrame) and endpoint in targets:
                the_df.sort_values(by=['kf_id'],inplace=True)
                PandasCsvUpdater(endpoint,the_df).update_csv_with_df()

if __name__ == "__main__":
    main()