# Check if submission_tools directory already exists
if [ ! -d "submission_tools" ]; then
    # OSF cli tool grabbing the submission script and the cv reference files
    osf -p bq6k9 clone submission_tools

    # Moves submission script and reference files to root directory 
    mv submission_tools/osfstorage/prepare_C2M2_submission.py .
    mv submission_tools/osfstorage/external_CV_reference_files .
fi

# Fetch C2M2 data package if it doesn't exist
if [ ! -f "C2M2_datapackage.json" ]; then
    osf -p c63aw fetch osfstorage/C2M2_datapackage.json
fi