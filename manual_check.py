import pandas as pd
import os
from os import DirEntry

def is_tsv(file : DirEntry):
    tsv_check = file.is_file() and file.name.endswith('.tsv')
    return tsv_check

data_frame_dict = {}
with os.scandir("./frictionless_validation/data") as directory:
    tsv_s = filter(is_tsv,directory)
    for tsv in tsv_s:
        # print(tsv)
        data_frame_dict.update({tsv.name : pd.read_table(tsv)})


subject_df = data_frame_dict['subject.tsv']

sex = set(subject_df['sex'])
print(sex)

# assert 'assay_type' in data.columns.values

# reduced = pd.DataFrame().assign(project_id=data['project_local_id'],file_format=data['file_format'])

# unique_proj_ids = set(reduced['project_id'])

# for id in unique_proj_ids:
#     print(id)