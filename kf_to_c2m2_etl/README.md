# Submission ETL Description
This ETL (Extract, Transform, Load) pipeline is designed to convert data from the Kids First (KF) data model to the National Institutes of Health's (NIH) Common Metadata Model (C2M2) data model, with a focus on human subjects research related to cancer and birth defects. The KF data model is used to store clinical and genomic data related to pediatric cancer and birth defects research, while the NIH's C2M2 Crosscut Metadata Model is a standardized data model used for cancer and other disease research data integration and analysis.

The pipeline consists of several Python scripts, including **transform.py**, which contains the transformation logic, and utilizes various libraries, including pandas for data manipulation and file handling operations. The source data is located in the "ingested" folder, and the converted data is stored in the "converted" folder. The data conversion is performed based on conversion tables and column mappings that are stored as TSV (Tab-Separated Values) files.

# ETL Flow
1. File Detection: The pipeline scans the "ingested" folder for input data files and identifies the files to be processed based on their file extension.

2. Column Mapping: The pipeline retrieves the column mapping for the target column from the "column_mapping.tsv" file. The column mapping maps the KF column name to the corresponding C2M2 column name in the NIH's C2M2 Crosscut Metadata Model.

3. Conversion Table Retrieval: The pipeline retrieves the conversion table for the target column from the appropriate TSV file based on the C2M2 column name retrieved from the column mapping.

4. Value Conversion: The pipeline reads the target column data from the input DataFrame and merges it with the conversion table. The merge is performed based on the "name" column in the conversion table and the target column in the input DataFrame. The resulting merged DataFrame contains the converted values in the C2M2 column, which are then stored in the input DataFrame using the KF column name retrieved from the column mapping.

5. Anatomy/More nuanced Mapping: The pipeline performs additional mapping for the "composition_term" column to lowercase using an OrderedDict object that maps the composition terms to their corresponding anatomical entities in lowercase format.

The ETL flow described above is performed for each target column in the input DataFrame, allowing for the conversion of multiple columns from the KF data model to the NIH's C2M2 Crosscut Metadata Model in a single run of the pipeline. This pipeline is intended to facilitate data integration and analysis for human subjects research related to cancer and birth defects, and can be used to convert data from diverse sources into a standardized format for further analysis and insights.