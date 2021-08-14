# Avalanche-etl
Python scripts for ETL (extract, transform and load) jobs for Avalanche's c-chain blocks, transactions, ERC20 / ERC721 tokens, transfers, receipts, logs, contracts, internal transactions.

** Currently supports blocks, transactions and token transfers.

# Instructions

1 - Create postgres database & run postgres service

2 - Apply settings through environment variables or through the .env file

3 - Register the database schema

Terminal into  ``scripts`` folder, then run:
```
python schema_registry.py
```
4 - Run block Service 

To start the service, run:
```
python start.py
```

**Prerequisites**:

- Untested below Python 3.8+

