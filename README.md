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

1. After cloning repo, navigate into project directory.

```bash
cd c2m2-submission-process
```

2. Set up virtual environment and stage files

```bash
python3 -m venv .

source bin/activate 

pip install -r requirements

python prepare_C2M2_submission.py 
```

3. Run Frictionless Validator

***Note*** Frictionless: Data management framework for Python that provides functionality to describe, extract, validate, and transform tabular data (DEVT Framework). 

First, create a directory for auto-gen'd and draft submission files.

```bash
mkdir frictionless_validate_combined_output
```

Copy tsv files from auto-gen and draft submission into combined output directory.

```bash
cp autogenerated_C2M2_term_tables/*.tsv             frictionless_validate_combined_output
cp draft_C2M2_submission_TSVs/*.tsv                 frictionless_validate_combined_output
cp draft_C2M2_submission_TSVs/C2M2_datapackage.json frictionless_validate_combined_output
```
Navigate to combined output directory and execute frictionless validator.

```bash
cd frictionless_validate_combined_output 

frictionless validate C2M2_datapackage.json > frictionless_report.txt
```

4. Install C2M2 submission code and re-run submission

Steps tbd.


