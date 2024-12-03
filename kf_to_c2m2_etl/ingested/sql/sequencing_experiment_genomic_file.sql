select * 
from sequencing_experiment_genomic_file segf
left join biospecimen_genomic_file on segf.genomic_file_id = biospecimen_genomic_file.genomic_file_id
left join biospecimen on biospecimen_genomic_file.biospecimen_id = biospecimen.kf_id
left join participant on biospecimen.participant_id = participant.kf_id
where participant.study_id != "SD_BHJXBDQK";
