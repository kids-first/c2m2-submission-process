import os
import json


c2m2_model_tables = ['project_in_project','project','subject_role_taxonomy','biosample_disease'
                     'biosample_from_subject','biosample','subject']

def get_table_cols_from_c2m2_json(table_name):
    json_path = os.path.join(os.getcwd(),'draft_C2M2_submission_TSVs','C2M2_datapackage.json')

    table_fields = []
    data = json.load(open(json_path,'r'))
    for resource in data.get('resources'):
        if resource.get('name') == table_name:
            for field in resource.get('schema').get('fields'):
                table_fields.append(field.get('name'))
    return table_fields 