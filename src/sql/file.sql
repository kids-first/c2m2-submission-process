select
  f.id_namespace,
  f.local_id,
  f.project_id_namespace,
  f.project_local_id,
  f.persistent_id,
  f.creation_time,
  f.size_in_bytes,
  -- f.uncompressed_size_in_bytes,
  f.sha256,
  f.md5, -- deprecated but allowed
  f.filename,
  f.file_format,
  -- f.compression_format,
  f.data_type,
  f.assay_type,
  -- f.analysis_type,
  -- f.mime_type,
  -- f.bundle_collection_id_namespace,
  -- f.bundle_collection_local_id,
  f.dbgap_study_id,
  f.access_url
from c2m2_file as f;
