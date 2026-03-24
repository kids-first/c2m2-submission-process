select
  c.id_namespace,
  c.local_id,
  --c.persistent_id,
  --c.creation_time,
  --c.abbreviation,
  c.name,
  c.description,
  c.has_time_series_data
from c2m2_collection as c;
