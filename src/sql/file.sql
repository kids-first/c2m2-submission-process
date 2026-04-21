select
  c2m2_file.id_namespace,
  c2m2_file.local_id,
  c2m2_file.project_id_namespace,
  c2m2_file.project_local_id,
  c2m2_file.persistent_id,
  c2m2_file.creation_time,
  c2m2_file.size_in_bytes,
  -- c2m2_file.uncompressed_size_in_bytes,
  c2m2_file.sha256,
  -- c2m2_file.md5, -- deprecated but allows
  c2m2_file.filename,
  c2m2_file.file_format,
  -- c2m2_file.compression_format,
  c2m2_file.data_type,
  c2m2_file.assay_type,
  -- c2m2_file.analysis_type,
  -- c2m2_file.mime_type,
  -- c2m2_file.bundle_collection_id_namespace,
  -- c2m2_file.bundle_collection_local_id,
  c2m2_file.dbgap_study_id,
  c2m2_file.access_url
from c2m2_file;
