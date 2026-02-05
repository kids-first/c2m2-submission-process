<p align="center">
  <img src="docs/kids_first_logo.svg" alt="Kids First repository logo" width="660px" />
</p>
<p align="center">
  <a href="https://github.com/kids-first/kf-template-repo/blob/master/LICENSE"><img src="https://img.shields.io/github/license/kids-first/kf-template-repo.svg?style=for-the-badge"></a>
</p>

### Repo Description

This repository will initially serve as a staging point for the source and data files associated with the C2M2 submission process. It might eventually grow into a pipeline for the C2M2 process, but we are starting small.

### Important Links

1. [Base Wiki Page for C2M2 Submissions](https://github.com/nih-cfde/published-documentation/wiki/Quickstart)
2. [Submission Prep Script Wiki](https://github.com/nih-cfde/published-documentation/wiki/submission-prep-script)
3. [C2M2 Submission Prep Script](https://osf.io/c67sp)
4. [C2M2 Table Summary](https://github.com/nih-cfde/published-documentation/wiki/C2M2-Table-Summary)
5. [CV Reference Files](https://osf.io/bq6k9/files/osfstorage)
6. [JSON Schema document describing the current C2M2 metadata model](https://osf.io/c63aw/)
7. [Frictionless: Data management framework for Python](https://pypi.org/project/frictionless/)
8. [OSF Client: Cli tool for grabbing OSF artifacts](https://osfclient.readthedocs.io/en/latest/)
9. [CFDE Submission Doc](https://docs.nih-cfde.org/en/latest/cfde-submit/docs/)

### Submission Process Steps

1. Set up your environment

If you have not yet, create a virtual environment by running the following command in your terminal:
```bash
python3.12 -m venv .venv
```
Then, activate your virtual environment:
```bash
source ./.venv/bin/activate
```
If you have not previously installed the package dependencies, do so by running:
```bash
pip install .
```

2. Run the pipeline end-to-end by running:
```bash
source ./src/pipeline.sh
```
This script will execute the following code:
```bash
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
```

3. Submit data to CFDE portal

Upload the C2M2 zip file via the CFDE Data Portal at https://data.cfde.cloud/submit/form.
