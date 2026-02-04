

# fill in your tables however you like
python3.12 ./src/sql_to_tsv_stream.py
python3.12 ./src/loader.py

# create an empty c2m2 submission OR update your existing submission directory
cd submission
cfde-c2m2 init

# finish preparing your package by resolving iris
cfde-c2m2 prepare

# verify integrity of your package
cfde-c2m2 validate

# zip the necessary files for a bare minimum package
cfde-c2m2 package