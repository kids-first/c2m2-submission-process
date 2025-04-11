import pandas as pd

from cfde_convert import uberon_mapping_dict
from etl_types import ETLType

def convert_days_to_years(days):
    if days:
        return f"{(days / 365):.02f}"

def modify_dbgap(study_id):
    if study_id and isinstance(study_id, str):
        if study_id.startswith('phs'):
            return study_id.split('.')[0]
        else:
            return ''

def get_access_url(study, did, md5):
    if isinstance(study,str) and isinstance(did,str) and pd.notnull(md5):
        if study not in ['SD_BHJXBDQK','SD_8Y99QZJJ','SD_46RR9ZR6','SD_YNSSAPHE']:
            return 'drs://data.kidsfirstdrc.org/' + did

def path_to_filename(path):
    if isinstance(path,str):
        return path.split('/')[-1]
		

def apply_kf_uberon_mapping(source_text, uberon_id):
    if isinstance(uberon_id,str) and uberon_id.lower().startswith('uberon'):
        return uberon_id
    elif isinstance(source_text,str):
        for anatomy_term, id in uberon_mapping_dict.items():
            if anatomy_term in source_text.lower():
                return id.upper()

def apply_fhir_uberon_mapping(uberon_id):
	if isinstance(uberon_id,str):
            if uberon_id.lower().startswith('uberon'):
                return uberon_id
            else:
                for anatomy_term, id in uberon_mapping_dict.items():
                    if anatomy_term in uberon_id.lower():
                        return id.upper()
             

def apply_uberon_mapping(etl_type: ETLType, *args):
    if not isinstance(etl_type,ETLType):
        raise Exception("ETL type not provided")
    if etl_type == ETLType.FHIR:
        return apply_fhir_uberon_mapping(args[0])
    elif etl_type == ETLType.DS:
        return apply_kf_uberon_mapping(args[0],args[1])