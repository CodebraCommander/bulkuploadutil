# Bulk Upload Utility

A Python utility for validating and manipulating redIQ bulk upload zip files.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

The utility provides three main commands: `validate`, `subset`, and `split`.

### Validate Command

Validates a bulk upload zip file and provides detailed information about its contents and any issues found.

```bash
python bulkuploadutil.py validate input.zip
```

#### Output
- Data Summary:
  - Total number of properties, line items, and history entries
  - Count of valid properties, line items, and history entries
  - Data relationships (properties/line items with/without history)
- Validation Issues (if any):
  - Missing Columns: Required columns missing from files
  - Missing Data: Required fields missing from rows
  - Duplicate IDs: Duplicate EntityIDs or LineItemIds
  - Invalid Data: Invalid values in required fields
  - Invalid References: References to non-existent properties or line items
  - Duplicate History: Duplicate history entries

### Subset Command

Creates a new zip file containing a subset of properties from the input file.

```bash
python bulkuploadutil.py subset input.zip output.zip num_properties
```

#### Parameters
- `input.zip`: Input bulk upload zip file
- `output.zip`: Output zip file path
- `num_properties`: Number of properties to include in the subset

#### Output
- Creates a new zip file containing:
  - The specified number of properties
  - All line items referenced by those properties
  - All history entries for those properties

### Split Command

Splits a bulk upload zip file into multiple smaller files, each containing a specified number of properties.

```bash
python bulkuploadutil.py split input.zip output_prefix batch_size [--output_dir OUTPUT_DIR]
```

#### Parameters
- `input.zip`: Input bulk upload zip file
- `output_prefix`: Prefix for output files (will be appended with _1.zip, _2.zip, etc.)
- `batch_size`: Number of properties per output file
- `--output_dir` or `-o`: (Optional) Directory to save output files

#### Output
- Creates multiple zip files named `output_prefix_1.zip`, `output_prefix_2.zip`, etc.
- Each file contains:
  - The specified number of properties
  - All line items referenced by those properties
  - All history entries for those properties
- If an output directory is specified, creates it if it doesn't exist

## File Format Requirements

The utility expects zip files containing three TSV (tab-separated values) files:

1. Property file (e.g., `property_20200101.txt`):
   - Required columns: EntityID, DealName

2. Line items file (e.g., `lineItems_20200101.txt`):
   - Required columns: LineItemId, LineItemDescription, redIQChartOfAccount, IsExpenseAccount

3. Historical file (e.g., `historical_20200101.txt`):
   - Required columns: EntityId, LineItemId, Date, IsAnnual, Value

## Notes

- EntityIDs and LineItemIds are treated as case-insensitive
- The utility preserves all original columns and data in output files
- All files are expected to be UTF-8 encoded

