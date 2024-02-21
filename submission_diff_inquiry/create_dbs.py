import os
import pandas as pd
import sqlite3
from argparse import ArgumentParser

def tsvs_to_sqlite(db_name_origin, db_name, file_type: str, directory: str):
    # Derive the database name from the directory name
    if db_name:
        db_name = "./submission_diff_inquiry/" + db_name + ".db"
    else:
        path_index = -1 if db_name_origin == "end_of_path" else 0
        root_dir = directory.split('/')[path_index] if '/' in directory else directory
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
    parser.add_argument("--name-origin",default="start_of_path",type=str, help='get db name from start or end of data file path')
    parser.add_argument("--db-name",type=str, help='Set db name')
    args = parser.parse_args()
    return args

# List of directories
args = get_cli_args()  # Replace with your list of directories

for directory in args.dirs:
    tsvs_to_sqlite(args.name_origin, args.db_name, args.file_type, directory)
