# CFDE Quarterly Submissions Analysis

This subproject contains scripts and Jupyter notebooks designed to facilitate the comparison of changes in raw counts for quarterly CFDE submissions. To conduct an analysis with these tools, the data packages for which analysis is desired must be obtained from the [cfde portal](https://app.nih-cfde.org/).    

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

It is recommended to create a virtual environment in this directory to isolate Submission Analysis dependencies from the ETL dependencies. Use 'pip' to install these (requirements file is included for convenience): 

- pandas

## How to Use:

1. Set up your environment in VSCode and ensure the necessary extensions (SQLite, SQLite Viewer, Jupyter) are installed.
2. Download your data packages from the CFDE portal.
    - At this site, there is a drop-down at the top of the page labeled 'For Submitters'. At the bottom of this list, there is a 'List All Submissions' option.
    - From there you can browse the data packages and download them with the 'raw data' link.
    - Next you would move the zip into the ***submission_diff_inquiry*** directory, where you would then unzip the data package preferably into a directory named after the quarter or year_quarter in which it was submitted.

3. Use the provided SQL scripts with the SQLite extension to perform quick checks on your data packages.

OR

3. For in-depth analyses, open the Jupyter notebooks and follow the instructions therein.
