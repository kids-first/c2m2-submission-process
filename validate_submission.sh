# Move to the validation directory
cd frictionless_validation

# Generates the validation report 
# TODO: Update filename to be dynamic
# Example name: frictionlessreport_c2m2_submission_year3_q1_v1.txt
frictionless validate C2M2_datapackage.json > ../frictionless_report.txt
 
