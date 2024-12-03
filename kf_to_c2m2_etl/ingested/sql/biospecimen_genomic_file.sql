select *
from biospecimen_genomic_file
left join biospecimen on biospecimen_genomic_file.biospecimen_id = biospecimen.kf_id
left join participant on biospecimen.participant_id = participant.kf_id
where participant.study_id != "SD_BHJXBDQK";
