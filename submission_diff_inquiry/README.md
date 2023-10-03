# CFDE Quarterly Submissions Analysis

This subproject contains scripts and Jupyter notebooks designed to facilitate the comparison of changes in raw counts for quarterly CFDE submissions.

## Files:

### SQL Scripts:
The provided SQL scripts can be used to perform various data checks and analyses on the data packages downloaded from the CFDE portal. By utilizing the SQLite extension in VSCode, users can quickly run these scripts against their data packages and view the results in an integrated environment.

### Python Scripts:
- **create_dbs.py**: A Python script to set up the necessary databases for the analysis.

### Jupyter Notebooks:
- **Y23_Q4_inquiry.ipynb**: This notebook provides an in-depth inquiry into the CFDE submissions for the fourth quarter of Year 23. It contains analyses, visualizations, and insights into the raw counts and other aspects of the data.
- **submission_diff_check.ipynb**: Focuses on identifying and visualizing differences between CFDE submissions over time. It helps in understanding the variations and trends in the data across different quarters.

## Tech Stack & Tools Required:

### VSCode Extensions

To work with this subproject effectively, it's recommended to use [Visual Studio Code (VSCode)](https://code.visualstudio.com/) as the primary editor. Here are the necessary extensions for VSCode:

- [SQLite](https://marketplace.visualstudio.com/items?itemName=alexcvzz.vscode-sqlite) - Provides SQLite integration into VSCode. With this extension, users can run the provided SQL scripts against their data packages for quick checks.
- [SQLite Viewer](https://marketplace.visualstudio.com/items?itemName=qwtel.sqlite-viewer) - Allows for easy viewing of SQLite databases within VSCode. It aids in visualizing the data and results of the SQL scripts.
- [Jupyter](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) - Offers Jupyter notebook integration in VSCode for interactive Python development and data analysis.

### Python Dependencies

It is recommended to create a virtual environment in this directory to isolate Submission Analysis dependencies from the ETL dependencies. The python package installer pip would be a good tool for obtaining these dependencies.

- pandas

## How to Use:

1. Set up your environment in VSCode and ensure the necessary extensions (SQLite, SQLite Viewer, Jupyter) are installed.
2. Download your data packages from the CFDE portal.
3. Use the provided SQL scripts with the SQLite extension to perform quick checks on your data packages.
4. For in-depth analyses, open the Jupyter notebooks and follow the instructions therein.
