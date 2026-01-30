import os

import pandas as pd

DEFAULT_TSV_DIR = "./submission"

class TsvLoader:
    def __init__(self): 
        self.tsv_dir = DEFAULT_TSV_DIR

    def update_tsvs(self):
        get_dcc_df().to_csv(os.path.join(self.tsv_dir,'dcc.tsv'),sep='\t',index=False)
        get_id_namespace().to_csv(os.path.join(self.tsv_dir,'id_namespace.tsv'),sep='\t',index=False)
        add_project_title_row(os.path.join(self.tsv_dir,'project.tsv'))

def get_dcc_df():
    return pd.DataFrame([
        {'id':'cfde_registry_dcc:kidsfirst',
         'dcc_name':'The Gabriella Miller Kids First Pediatric Research Program',
         'dcc_abbreviation':'KFDRC',
         'dcc_description':'A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.',
         'contact_email':'support@kidsfirstdrc.org',
         'contact_name':'Kids First Support',
         'dcc_url':'https://kidsfirstdrc.org',
         'project_id_namespace':'kidsfirst:',
         'project_local_id':'drc',
         }
    ])

def get_id_namespace():
    return pd.DataFrame([
        {'id':'kidsfirst:',
         'abbreviation':'KFDRC_NS',
         'name':'The Gabriella Miller Kids First Pediatric Research Program',
         'description':'A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.',
         }
    ])

def add_project_title_row(project_file_path):
    project_title_row = {'id_namespace':'kidsfirst:',
                         'local_id':'drc',
                         'persistent_id':'',
                         'creation_time':'',
                         'abbreviation':'KFDRC',
                         'name':'The Gabriella Miller Kids First Pediatric Research Program',
                         'description':'''A large-scale data resource to help researchers uncover new insights into the biology of childhood cancer and structural birth defects.'''}

    project_df = pd.read_csv(project_file_path,sep='\t')
    project_df = pd.concat([pd.DataFrame(project_title_row,index=[0]),project_df]).reset_index(drop=True)
    project_df.to_csv(project_file_path,sep='\t',index=False)

if __name__ == "__main__":
    loader = TsvLoader()
    loader.update_tsvs()
