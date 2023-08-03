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

fhir_resource_mapping = {"participants":"Patient"}

def get_fhir_mapping(entity_name: str) -> List[tuple]:
    mapping_path = os.path.join(file_locations.get_fhir_mapping_paths(),f'{entity_name}_mapping.tsv')
    mapping_df = pd.read_table(mapping_path)
    return mapping_df.to_records(index=False).tolist()

class FhirIngest:
    study_descendants = ["participants"]

    def __init__(self,studies_to_ingest):
        self.studies = pd.DataFrame({ "studies": studies_to_ingest })


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

        return {','.join(self.studies['studies'].to_list()): df_dict}