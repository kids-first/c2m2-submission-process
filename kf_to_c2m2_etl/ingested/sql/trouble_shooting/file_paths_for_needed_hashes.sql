select  file.local_id as 'genomic_file_id', file.project_local_id as 'study_id', gf.external_id as 'file_path'
from file
join genomic_file gf on gf.kf_id = file.local_id
where file.size_in_bytes=0
and file.project_local_id not in ('SD_46RR9ZR6','SD_8Y99QZJJ','SD_BHJXBDQK','SD_YNSSAPHE');