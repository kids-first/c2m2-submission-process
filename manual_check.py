import pandas as pd
import os
from os import DirEntry

def get_table_from_user() -> str:
    user_input = input("What table would you like to see? ")
    return user_input

def is_tsv(file : DirEntry):
    tsv_check = file.is_file() and file.name.endswith('.tsv')
    return tsv_check

data_frame_dict = {}
with os.scandir("./frictionless_validation") as directory:
    tsv_s = filter(is_tsv,directory)
    for tsv in tsv_s:
        print(tsv)
        data_frame_dict.update({tsv.name : pd.read_table(tsv)})


while user_input := get_table_from_user():
    user_selected_df = data_frame_dict[user_input + '.tsv']
    print(user_selected_df.head())
    
     
    