<p align="center">
  <img src="docs/kids_first_logo.svg" alt="Kids First repository logo" width="660px" />
</p>
<p align="center">
  <a href="https://github.com/kids-first/kf-template-repo/blob/master/LICENSE"><img src="https://img.shields.io/github/license/kids-first/kf-template-repo.svg?style=for-the-badge"></a>
</p>

# Kids First Repository Template

Use this template to bootstrap a new Kids First repository.

### Badges

Update the LICENSE badge to point to the new repo location on GitHub.
Note that the LICENSE badge will fail to render correctly unless the repo has
been set to **public**.

Add additional badges for CI, docs, and other integrations as needed within the
`<p>` tag next to the LICENSE.

### Repo Description

This repository will initially serve as a staging point for the source and data files associated with the C2M2 submission process. It might eventually grow into a pipeline for the C2M2 process, but we are starting small.

"A journey of a thousand miles begins with a single step" -Laozi

### Important Links

1. [Base Wiki Page for C2M2 Submissions](https://github.com/nih-cfde/published-documentation/wiki/Quickstart)
2. [Submission Prep Script Wiki](https://github.com/nih-cfde/published-documentation/wiki/submission-prep-script)
3. [Link to C2M2 Submission Prep Script](https://osf.io/c67sp)
4. [Link to CV Reference Files](https://osf.io/bq6k9/files/osfstorage)
4. [Link for JSON Schema document describing the current C2M2 metadata model](https://osf.io/c63aw/)
5. [Frictionless: Data management framework for Python](https://pypi.org/project/frictionless/)
6. [OSF Client: Cli tool for grabbing OSF artifacts](https://osfclient.readthedocs.io/en/latest/)

### Submission Process Steps

1. Set up directory and stage files

```bash
python3 -m venv virtualenvY3Q1

source virtualenvY3Q1/bin/activate 

pip install --upgrade pip 

cd c2m2_helper_year3_q1 

python prepare_C2M2_submission.py 
```
2. Run Frictionless Validator

***Note*** Frictionless: Data management framework for Python that provides functionality to describe, extract, validate, and transform tabular data (DEVT Framework). 

```bash
pip install frictionless 

cd frictionless_validate_combined_output 

frictionless validate C2M2_datapackage.json > ~frictionlessreport_c2m2_submission_year3_q1_v1.txt 
```

3. Install C2M2 submission code and re-run submission

Steps tbd.


