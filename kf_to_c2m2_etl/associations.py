import pandas as pd

# from fhir_ingest import FhirIngest


fhir_resource_mapping = {"participant":"Patient",
                         "biospecimen":"Specimen",
                         "genomic-file":"DocumentReference"}

associations = {"Specimen":{"patient_id":"participant_id"}}

entity_by_prefix = {"PT":"participant",
                    "BS":"biospecimen",
                    "GF":"genomic-file"}

def find_kf_entity_type(the_df: pd.DataFrame):
    if "kf_id" in the_df.columns:
        if ((row_index := the_df['kf_id'].first_valid_index()) is not None):
            prefix = the_df['kf_id'].loc[row_index].split('_')[0]
            return entity_by_prefix.get(prefix)
    raise BaseException

def clean_association_column(the_df: pd.DataFrame):
    if fhir_resource_type := fhir_resource_mapping.get(find_kf_entity_type(the_df)):
        if associations.get(fhir_resource_type):
            for association in associations[fhir_resource_type].keys():
                the_df[association] = the_df[association].apply(lambda the_col: the_col.split('/')[-1])
    
    return the_df

class AssociationBuilder:
    """
    Purpose of this class is to reestablish the associations for the kf data model 
    from the associations of the FHIR resources. The goal will be to add the foreign
    key column back, and removing the fhir specific ids.
    """

    def __init__(self, df_with_association, df_with_id) -> None:
        self.df_with_association = df_with_association
        self.df_with_id = df_with_id


    def establish_association(self):
        self.df_with_association = clean_association_column(self.df_with_association)

        fhir_resource = fhir_resource_mapping[find_kf_entity_type(self.df_with_association)]
        
        for association in associations[fhir_resource]:

            self.df_with_association = self.df_with_association.merge(self.df_with_id[['kf_id','fhir_id']],
                                                                      how='inner',
                                                                      left_on=association,
                                                                      right_on='fhir_id')

            self.df_with_association.drop([association,'fhir_id'],axis=1,inplace=True)
            self.df_with_association.rename(columns={'kf_id_x':'kf_id',
                                                     'kf_id_y':associations[fhir_resource].get(association)},
                                            inplace=True)
            
        return self.df_with_association


# fhir_ingest = FhirIngest(["SD_DYPMEHHF"])

# extract_data = fhir_ingest.extract()
# top_level_key = next(iter(extract_data))

# entity_dict = extract_data[top_level_key]

# biospec_df = AssociationBuilder(entity_dict['biospecimens'],entity_dict['participants']).establish_association()
# print(biospec_df)