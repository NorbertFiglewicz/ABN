# KommatiPara Client Data Collation

## Description

This Python project is created to consolidate and process two distinct datasets that include details about clients and their financial information for an imaginary company named KommatiPara, specializing in bitcoin trading. The objective of this project is to generate a fresh dataset comprising client emails from the United Kingdom and the Netherlands, along with pertinent financial details, in order to facilitate a marketing campaign.

## RUN

Parameter description:

```bash
python -m main --help
```

- `<client_data_file_path>`: The path to the CSV file containing client information.
- `<financial_data_file_path>`: The path to the CSV file containing financial information.
- `<country_filter>`: The country filter to specify the target countries (e.g., "UK" or "Netherlands").

```bash
python -m main --help
```

---
Install the project dependencies using:

```bash
python -m pip install -r requirements.txt
```

Run the project using the following:

```bash
python src/pyspark_app/main.py --clients_file <client_data_file_path> --financials_file <financial_data_file_path> --countries <country_filter>
```

Example:
```bash
python src/pyspark_app/main.py --clients_file "raw_data/dataset_one.csv" --financials_file "raw_data/dataset_two.csv" --countries "Netherlands" "United Kingdom"
```

## Data Processing
1. Read mandatory columns from first (personal) file.
| id | email | country |
|----|-------|---------|

2. Read mandatory columns from the second file (financial) .
| id | btc_a | cc_t |
|----|-------|------|
3. Join sets, remove one duplicated id column.
4. The data is filtered to specified countries (United Kingdom and Netherlands).
5. Renamed columns, as follows:
    - btc_a -> bitcoin_address
    - cc_t -> credit_card_type
    - id -> client_identifier
5. Saved in the client_data directory.

## Logging
The application store logs in `logs/` directory, with a rotation policy.