import pandas as pd
import os
from os import DirEntry
from argparse import ArgumentParser

def main():
    args = get_args()
    data_frame_dict = {}
    with os.scandir(args.path) as directory:
        tsv_s = filter(is_tsv,directory)
        tsv_list = list(tsv_s)
        tsv_list.sort(key = lambda item : item.name)
        for tsv in tsv_list:
            print(tsv)
            data_frame_dict.update({tsv.name : pd.read_table(tsv)})

    while user_input := get_table_from_user():
        tsv = user_input + '.tsv'
        if tsv in data_frame_dict.keys():
            user_selected_df = data_frame_dict[tsv]
            print(user_selected_df.head())
        else:
            print("Not a valid table!!!")

    print('*' * 80)
    print('UNEMPTY TABLES') 
    for tablename, df in data_frame_dict.items():
        if not df.empty:
            print(tablename)

    print('*' * 80)
    print('EMPTY TABLES') 
    for tablename, df in data_frame_dict.items():
        if df.empty:
            print(tablename)


def get_table_from_user() -> str:
    user_input = input("What table would you like to see? ")
    return user_input

def is_tsv(file : DirEntry):
    tsv_check = file.is_file() and file.name.endswith('.tsv')
    return tsv_check

def get_args():
    parser = ArgumentParser(description="Tool for quickly checking table heads.")
    parser.add_argument("--path",dest="path",default=".",help="valid options: path to directory containing tsv's")
    return parser.parse_args()

if __name__ == "__main__":
    main() 