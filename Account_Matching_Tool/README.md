# Account Matching Tool README

This tool was written to assist the identification of existing customers within our Salesforce instance. Data Governance assists other departments in automation data to expedite large customer onboarding efforts. The purpose of this tool is to take an input spreadsheet of customers to find. Then the tool digests the table, standardizes it, and identifies the best possible matches on accounts existing in our Salesforce instance. The tool will return a CSV of best possible matches, as well as input accounts that do not seem to exist as Salesforce records.

## Package Dependencies
    - Pandas >= 2.0.2
    - numpy >= 1.24.3
    - SQL Alchemy >= 2.0.15
    - rapidfuzz >= 3.1.1

## Instructions
    1. Download and install source code to directory of choice. Make sure to create a directory to house the downloaded files.
    2. Make sure the package dependencies are installed and accessible via environment variables.
    3. Create an INPUT and OUTPUT folder in your tool directory.
    4. Update SF_Query.py with the query you need to use to query your database.
    5. Update INO_SQL_Creds.py with the your business creditials for accessing SQL data.
    6. Place an Excel spreadsheet you want to identify account matches for into the INPUT folder.
    7. Open up Powershell in the main tool directory. Execute "python main.py -n [FILENAME]"
        1. Add the -f flag if you'd like to process Account Name and Address fuzzy matches
        2. Add the -a flag if you'd like to receive all matches, instead of the best 3 matches.
    8. Wait until tool finishes running, then open up the file created in the OUTPUT folder to view results.

## Input Table Template
