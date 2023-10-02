select distinct f.project_local_id
from file f
join files_with_visibility fwv on fwv.file_id = f.local_id 
where fwv.file_visibility = False;