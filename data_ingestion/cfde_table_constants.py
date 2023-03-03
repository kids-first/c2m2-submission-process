
cfde_table_columns_dict = {'project':
                           ['id_namespace','local_id','persistent_id','creation_time','abbreviation','name','description'], 
                            'subject_role_taxonomy':
                           ['subject_id_namespace','subject_local_id','role_id','taxonomy_id'],
                           'biosample_disease':
                           ['biosample_id_namespace','biosample_local_id','association_type'], #missing disease column
                           'biosample_from_subject':
                           ['biosample_id_namespace','biosample_local_id','subject_id_namespace','subject_local_id','age_at_sampling'],
                           'biosample':
                           ['id_namespace','local_id','project_id_namespace','project_local_id','created_at','uberon_id_anatomical_site'],
                           'subject':
                           ['id_namespace','local_id','project_id_namespace','project_local_id','created_at','gender','ethnicity']
                           }