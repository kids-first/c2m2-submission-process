select
  id_namespace,
  local_id,
  project_id_namespace,
  project_local_id,
  persistent_id,
  creation_time,
  size_in_bytes,
  sha256,
  file_format,
  dbgap_study_id,
  data_type,
  assay_type,
  access_url
from nemarichc_dev_schema_reporting.c2m2_file;
