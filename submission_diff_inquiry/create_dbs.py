import os
import pandas as pd
import sqlite3
from argparse import ArgumentParser

def tsvs_to_sqlite(file_type: str, directory: str):
    # Derive the database name from the directory name
    root_dir = directory.split('/')[0] if '/' in directory else directory
    db_name = root_dir + '.db'
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_name)
    
    # List all .csv/.tsv files in the directory
    data_files = [f for f in os.listdir(directory) if f.endswith('.tsv' if file_type == 'tsv' else '.csv')]
    
    for data_file in data_files:
        # Read the .csv/.tsv file into a DataFrame
        separator = '\t' if file_type == 'tsv' else ','
        df = pd.read_csv(os.path.join(directory, data_file), sep=separator)
        
        # Use the file name (without extension) as the table name
        table_name = os.path.splitext(data_file)[0]
        
        # Write the DataFrame to the SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Close the database connection
    conn.close()

def get_cli_args():
    parser = ArgumentParser(prog='db Creator',
                            description='Creates dbs from TSVs')
    parser.add_argument("dirs", metavar="DIRECTORIES", type=str, nargs='+',help='a list of directories')
    parser.add_argument("--file-type", default="tsv", type=str, help='file extension of data tables')
    args = parser.parse_args()
    return args

# List of directories
args = get_cli_args()  # Replace with your list of directories

for directory in args.dirs:
    tsvs_to_sqlite(args.file_type, directory)
