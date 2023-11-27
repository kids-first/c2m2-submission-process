# Function to delete submission tools and datapackage
delete_files() {
    echo "Deleting submission_tools and C2M2_datapackage.json for refresh..."
    rm -rf submission_tools
    rm -f C2M2_datapackage.json
}

# Check for 'refresh' command-line argument
if [ "$1" = "refresh" ]; then
    delete_files
fi

# Check if submission_tools directory already exists
if [ ! -d "submission_tools" ]; then
    echo "Downloading submission tools. (submission prep script AND CV ref files.)"
    # OSF cli tool grabbing the submission script and the cv reference files
    osf -p bq6k9 clone submission_tools

    # Moves submission script and reference files to root directory 
    mv submission_tools/osfstorage/prepare_C2M2_submission.py .
    mv submission_tools/osfstorage/external_CV_reference_files .
else
    echo "submission tools already acquired!"
fi

# Fetch C2M2 data package if it doesn't exist
if [ ! -f "C2M2_datapackage.json" ]; then
    echo "Downloading c2m2 data package."
    osf -p c63aw fetch osfstorage/C2M2_datapackage.json
else
    echo "c2m2 data package already acquired!"
fi