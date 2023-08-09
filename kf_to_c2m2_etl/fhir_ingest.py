import os
from collections import defaultdict 

import pandas as pd
from typing import List


from fhir_pyrate import Ahoy, Pirate

from file_locations import file_locations


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

fhir_resource_mapping = {"participants":"Patient"}

def get_fhir_mapping(entity_name: str) -> List[tuple]:
    mapping_path = os.path.join(file_locations.get_fhir_mapping_paths(),f'{entity_name}_mapping.tsv')
    mapping_df = pd.read_table(mapping_path)
    return mapping_df.to_records(index=False).tolist()

class FhirIngest:
    study_descendants = ["participants"]

    def __init__(self):
        self.studies_df = FhirIngest.get_studies() 
        self.studies = pd.DataFrame({'studies': self.studies_df['kf_id'].to_list()})


    def extract(self) -> defaultdict:
        df_dict = defaultdict()
        
        for kf_entity in FhirIngest.study_descendants:
            the_df = pirate.trade_rows_for_dataframe(
                self.studies,
                resource_type=fhir_resource_mapping[kf_entity],
                df_constraints={"_tag":"studies"},
                fhir_paths=get_fhir_mapping(kf_entity)
            )

            df_dict.setdefault(kf_entity,the_df)

        df_dict.setdefault('studies',self.studies_df)

        return {','.join(self.studies['studies'].to_list()): df_dict}

    def get_studies():
        studies_df = pirate.steal_bundles_to_dataframe(
                resource_type='ResearchStudy',
                fhir_paths=get_fhir_mapping('studies')
        )
        studies_df = studies_df[~studies_df['kf_id'].isin(include_studies_to_exclude)]
        return studies_df