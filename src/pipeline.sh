#!/bin/bash

# fill in your tables however you like
python3.12 ./src/sql_to_tsv_stream.py

# create an empty c2m2 submission OR update your existing submission directory
cd submission || return
cfde-c2m2 init

# finish preparing your package by resolving iris
cfde-c2m2 prepare

# verify integrity of your package and export results to `logs` folder
cfde-c2m2 validate > "./logs/$(date +"%Y%m%d_%H%M%S")_c2m2_validation.log" 2>&1

# zip the necessary files for a bare minimum package
cfde-c2m2 package