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
3. [C2M2 Submission Prep Script](https://osf.io/c67sp)
4. [C2M2 Table Summary](https://github.com/nih-cfde/published-documentation/wiki/C2M2-Table-Summary)
4. [CV Reference Files](https://osf.io/bq6k9/files/osfstorage)
4. [JSON Schema document describing the current C2M2 metadata model](https://osf.io/c63aw/)
5. [Frictionless: Data management framework for Python](https://pypi.org/project/frictionless/)
6. [OSF Client: Cli tool for grabbing OSF artifacts](https://osfclient.readthedocs.io/en/latest/)

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

3. Execute osf script for preparing c2m2 submission 
 - Executes prepare submission script 
 - Creates frictionless validation directory 
 - Moves data files and generated files to validation directory
 - Moves the C2M2 file used to validate the submitted files

```bash
./prepare_c2m2_submission.sh
```

4. Validate C2M2 submission data
 - Move to the validation directory 
 - Generates the validation report 

 ```bash
./validate_submission.sh YEAR QUARTER VERSION
 ```

5. Submit data
 - tbd