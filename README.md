<p align="center">
  <img src="docs/kids_first_logo.svg" alt="Kids First repository logo" width="660px" />
</p>
<p align="center">
  <a href="https://github.com/kids-first/kf-template-repo/blob/master/LICENSE"><img src="https://img.shields.io/github/license/kids-first/kf-template-repo.svg?style=for-the-badge"></a>
</p>

### Repo Description

This repository will initially serve as a staging point for the source and data files associated with the C2M2 submission process. It might eventually grow into a pipeline for the C2M2 process, but we are starting small.

"A journey of a thousand miles begins with a single step" -Laozi

### Important Links

1. [Base Wiki Page for C2M2 Submissions](https://github.com/nih-cfde/published-documentation/wiki/Quickstart)
2. [Submission Prep Script Wiki](https://github.com/nih-cfde/published-documentation/wiki/submission-prep-script)
3. [C2M2 Submission Prep Script](https://osf.io/c67sp)
4. [C2M2 Table Summary](https://github.com/nih-cfde/published-documentation/wiki/C2M2-Table-Summary)
4. [CV Reference Files](https://osf.io/bq6k9/files/osfstorage)
4. [JSON Schema document describing the current C2M2 metadata model](https://osf.io/c63aw/)
5. [Frictionless: Data management framework for Python](https://pypi.org/project/frictionless/)
6. [OSF Client: Cli tool for grabbing OSF artifacts](https://osfclient.readthedocs.io/en/latest/)
7. [CFDE Submission Doc](https://docs.nih-cfde.org/en/latest/cfde-submit/docs/)

### Submission Process Steps

1. Evironment Setup 
 - Creates a virtual environment in the current directory
 - Activates the venv
 - Installs package dependencies from requirements.txt

 ```bash
source setup_evn.sh
 ```

2. Acquire submission tools from OSF
- OSF cli tool grabbing the submission script and the cv reference files
- Moves submission script and reference files to root directory 
- OSF cli tool grabbing the C2M2 data package to validate the submission

```bash
./acquire_osf_c2m2_submission_tools.sh
```

3. Execute kf to c2m2 etl process
- Execute transform script
  - The script executes in 3 phases
    1. Extract (Ingest class utilizing kf-utils writes kf model data in the form of tsv to the /kf_to_c2m2_etl/ingested/tables directory.)
    2. Transform (KF model data mapping to tables is transformed into c2m2 tables and written back out as tsv's to /kf_to_c2m2_etl/transformed.)
    3. Load (Moves transformed tsv's into directory in order to execute script contributing controlled vocabularies.)
     - Also, adds empty tables required for submission

```bash
python /kf_to_c2m2_etl/etl.py
```

4. Execute osf script for preparing c2m2 submission 
 - Executes prepare submission script 
 - Creates frictionless validation directory 
 - Moves data files and generated files to validation directory
 - Moves the C2M2 file used to validate the submitted files

```bash
./prepare_c2m2_submission.sh
```

5. Validate C2M2 submission data
 - Move to the validation directory 
 - Generates the validation report 

 ```bash
./validate_submission.sh YEAR QUARTER VERSION
 ```

6. Submit data to CFDE portal

*** Refer to Important Links #7 for additional info***
 - Login with submit tool 
 - Execute submission
   * Check tables for conformance to C2M2's latest release notes
   * Set DCC for submission (cfde_registry_dcc:kidsfirst)
 - Verify submission in progress
 - Review submission results

 ```bash
# Will be redirected to web browser for credentials
cfde-submit login 

# Command starts submission and sets data coordinating center
cfde-submit run path-to-frictionless-validation --ignore-git --dcc-id cfde_registry_dcc:kidsfirst 

# Can be executed intermittently to verify submission status
cfde-submit status

# Logout when submission is completed
cfde-submit logout
```