select
  p.id_namespace,
  p.local_id,
  p.persistent_id,
  p.creation_time,
  p.abbreviation,
  p.name,
  p.description
from c2m2_project as p;
