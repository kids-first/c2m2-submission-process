import os
import pandas as pd
import sqlite3
from argparse import ArgumentParser

def tsvs_to_sqlite(directory: str):
    # Derive the database name from the directory name
    root_dir = directory.split('/')[0] if '/' in directory else directory
    db_name = root_dir + '.db'
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_name)
    
    # List all .tsv files in the directory
    tsv_files = [f for f in os.listdir(directory) if f.endswith('.tsv')]
    
    for tsv_file in tsv_files:
        # Read the .tsv file into a DataFrame
        df = pd.read_csv(os.path.join(directory, tsv_file), sep='\t')
        
        # Use the file name (without extension) as the table name
        table_name = os.path.splitext(tsv_file)[0]
        
        # Write the DataFrame to the SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # Close the database connection
    conn.close()

def get_directories():
    parser = ArgumentParser(prog='db Creator',
                            description='Creates dbs from TSVs')
    parser.add_argument("dirs", metavar="DIRECTORIES", type=str, nargs='+',help='a list of directories')
    args = parser.parse_args()
    return args.dirs

# List of directories
directories = get_directories()  # Replace with your list of directories

for directory in directories:
    tsvs_to_sqlite(directory)
