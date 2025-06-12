# redIQ Bulk Upload Utility

This project provides a simple command line utility for working with bulk upload files for the redIQ system.  The utility can validate the contents of a bulk upload zip file and generate smaller sample data sets for testing.

## Usage

```
python bulkuploadutil.py validate PATH_TO_ZIP
```

The command checks that the required files exist in the zip archive (`property_yyyyMMdd.txt`, `lineItems_yyyyMMdd.txt`, and `historical_yyyyMMdd.txt`), verifies that required columns are present, and validates that historical data references existing property and line item IDs.

```
python bulkuploadutil.py subset PATH_TO_ZIP OUTPUT_ZIP NUM_PROPERTIES
```

The subset command creates a new zip archive containing only the first `NUM_PROPERTIES` properties and the corresponding line item and historical rows.

The resulting files are written using today's date in the file names.

