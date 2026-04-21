# Kids First CFDE C2M2 Submission Process

![Kids First DRC Logo](./docs/kids_first_logo.svg)

[![Kids First DRC CFDE C2M2 Submission Repo License](https://img.shields.io/github/license/kids-first/c2m2-submission-process.svg?style=for-the-badge)](https://github.com/kids-first/c2m2-submission-process/blob/master/LICENSE)

## Repo Description

This repository contains SQL scripts and Python code necessary to generate and package the Kids First Common Fund Data Ecosystem (CFDE) Cross-cut Metadata Model (C2M2) manifest. It queries tables from the D3b Data Warehouse and does some minimal transformations. The user should then utilize the CFDE `cfde-c2m2` CLI tool to prepare and validate the manifest. The user can then submit the zipped manifest package via the CFDE data portal.

## Submission Process Steps

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

2. Run the pipeline.

    You can run the entire generation process end-to-end by running the pipeline script:

    ```bash
    source ./src/pipeline.sh
    ```

    That script will execute the following code:

    ```bash
    # fill in your tables however you like
    python3.12 ./src/sql_to_tsv_stream.py

    # create an empty c2m2 submission OR update your existing submission directory
    cd submission || return
    cfde-c2m2 init

    # finish preparing your package by resolving iris
    cfde-c2m2 prepare

    # verify integrity of your package and export results to `logs` folder
    cfde-c2m2 validate > "../logs/$(date +"%Y%m%d_%H%M%S")_c2m2_validation.log" 2>&1

    # zip the necessary files for a bare minimum package
    cfde-c2m2 package
    ```

3. Submit data to CFDE portal

    Upload the C2M2 manifest zip file via the [CFDE Data Portal](https://data.cfde.cloud/submit/form).
