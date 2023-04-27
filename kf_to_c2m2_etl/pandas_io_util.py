import os
import pandas as pd

ingest_dir = os.path.join(os.getcwd(),'kf_to_c2m2_etl','ingest_test')


class PandasCsvUpdater:

    def __init__(self, table_name: str, the_df: pd.DataFrame):
        self.table_name = table_name
        self.the_df = the_df

    def update_csv_with_df(self) -> None:
        file_path = os.path.join(ingest_dir,f'{self.table_name}.csv')

        self.the_df.to_csv(file_path,
                           mode='a+',
                           header=not os.path.exists(file_path),
                           index=False)