import os
from collections import defaultdict 

import pandas as pd
from typing import List


from fhir_pyrate import Ahoy, Pirate

from pandas_io_util import PandasCsvUpdater


PROD_URL = 'https://kf-api-fhir-service.kidsfirstdrc.org/'

auth = Ahoy(
        username="wnkhan32@gmail.com",
        auth_method=None,
        auth_url=PROD_URL
    )

pirate = Pirate(
        auth=auth, 
        base_url=PROD_URL, 
        print_request_url=False, 
        num_processes=1
    )

include_studies_to_exclude = ['SD_T8VSYRSG','SD_FYCR78W0','SD_Y6VRG6MD','SD_65064P2Z']

fhir_resource_mapping = {"participants":"Patient",
                         "biospecimens":"Specimen",
                         "genomic-files":"DocumentReference"}



def convert_drs_uri_to_did(the_df : pd.DataFrame):
    if 'latest_did' in the_df.columns:
        the_df['latest_did'] = the_df['latest_did'].apply(lambda the_col: the_col.split('/')[-1])
    return the_df

class FhirIngest:
    study_descendants = ["participants","biospecimens"]
    fhir_resources = ['Patient','Specimen','DocumentReference']

    def __init__(self, selected_studies = None):

        # Get studies available on fhir server
        self.studies_df = FhirIngest._get_studies() 

        # Reduce study df to selected studies if supplied
        if selected_studies is not None:
            selected_studies_df = pd.DataFrame({'kf_id':selected_studies})

            self.studies_df = self.studies_df.merge(selected_studies_df,
                                                    how='inner',
                                                    left_on='identifier_0_value',
                                                    right_on='kf_id')

        self.studies = pd.DataFrame({'studies': self.studies_df['kf_id'].to_list()})


    def extract(self) -> defaultdict:
        df_dict = defaultdict()
        
        for fhir_resource in FhirIngest.fhir_resources:
            the_df = pirate.trade_rows_for_dataframe(
                self.studies,
                resource_type=fhir_resource,
                df_constraints={"_tag":"studies"},
            )

            df_dict.setdefault(fhir_resource,the_df)

        df_dict.setdefault('ResearchStudy',self.studies_df)


        return {','.join(self.studies['studies'].to_list()): df_dict}

    def _get_studies():
        studies_df = pirate.steal_bundles_to_dataframe(
                resource_type='ResearchStudy',
        )
        studies_df = studies_df[~studies_df['identifier_0_value'].isin(include_studies_to_exclude)]
        return studies_df


def write_fhir_studies_to_disk(studies_dict: dict):
    for index, (study, endpoints) in enumerate(studies_dict.items()):
        for endpoint, the_df in endpoints.items():
            if isinstance(the_df,pd.DataFrame):
                PandasCsvUpdater(endpoint,the_df).update_csv_with_df()


def get_fhir_resource_by_study(resource_name: str, study_list: list, fhir_paths = None) -> pd.DataFrame:
    the_df = pirate.trade_rows_for_dataframe(
            pd.DataFrame({'studies': study_list}),
            resource_type=resource_name,
            df_constraints={"_tag":"studies"},
            fhir_paths=fhir_paths
        )
    
    return the_df