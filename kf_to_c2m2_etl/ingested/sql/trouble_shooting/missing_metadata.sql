select study.kf_id, study.name, study.domain, study.program, bucket.bucket_name
from study
join
(select distinct(project_local_id)
from file
where file.size_in_bytes=0) as missing
on missing.project_local_id=study.kf_id
join bucket on bucket.study_id=study.kf_id
where bucket.is_dr = 'False'
order by kf_id;