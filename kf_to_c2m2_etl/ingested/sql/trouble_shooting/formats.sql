select distinct(f.file_format), gf.file_format
from genomic_file gf
join file f on gf.kf_id=f.local_id
order by gf.file_format asc;
