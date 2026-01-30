# Move to the validation directory
cd frictionless_validation

# Generates the validation report 
frictionless validate --limit-memory 10000 C2M2_datapackage.json > "../frictionlessreport_c2m2_submission_$1_$2_$3.txt"
 