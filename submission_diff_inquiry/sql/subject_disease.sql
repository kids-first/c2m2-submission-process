select --local_id, disease, project_local_id
distinct project_local_id
from subject_disease sd
join subject s on s.local_id = sd.subject_local_id
where sd.disease is NULL;