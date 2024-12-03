select * 
from biospecimen
left join participant on biospecimen.participant_id = participant.kf_id
where participant.study_id != "SD_BHJXBDQK";
