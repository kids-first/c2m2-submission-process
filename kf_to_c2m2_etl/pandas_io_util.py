import os
from glob import glob

import pandas as pd

from file_locations import file_locations

class PandasCsvUpdater:

    def __init__(self, table_name: str, the_df: pd.DataFrame):
        self.table_name = table_name
        self.the_df = the_df

    def update_csv_with_df(self) -> None:
        file_path = os.path.join(file_locations.get_ingested_path(),f'{self.table_name}.csv')

        self.the_df.to_csv(file_path,
                           mode='a+',
                           header=not os.path.exists(file_path),
                           index=False)


def delete_all_csvs(csv_s: list):
    for csv in csv_s:
        os.remove(csv)

def delete_selected_csv(csv_s: list):
    csv_dict = {csv.split('/')[-1]: csv for csv in csv_s if isinstance(csv, str)}
    csv_name = input(f"Enter the name of the CSV file you want to delete: {csv_dict.keys()}")
    
    # Find the filename that matches the entered string
    match = next((name for name in csv_dict.keys() if csv_name in name), None)
    
    # Delete the corresponding file and print a success message
    csv_path = csv_dict[match]
    os.remove(csv_path)
    print(f"'{match}' deleted successfully.")

execute_user_option = {'all': delete_all_csvs, 'spec': delete_selected_csv}

def handle_pre_existing_files():
    if csv_s := glob(os.path.join(file_locations.get_ingested_path(),'*.csv')):
        print('The ingest directory isn\'t empty.')

        user_opt = input('Delete all or specified: ')
        USER_OPTS = ['all','spec']
        user_opt = next((opt for opt in USER_OPTS if user_opt in opt), None)
        execute_user_option[user_opt](csv_s)