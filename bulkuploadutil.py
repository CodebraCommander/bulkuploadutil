# Utility to validate and manipulate redIQ bulk upload zip files
import argparse
import csv
import io
import re
import sys
import zipfile
import os
import logging
from tqdm import tqdm
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

PROPERTY_PATTERN = re.compile(r"property_\d{8}\.txt")
LINEITEM_PATTERN = re.compile(r"lineItems_\d{8}\.txt")
HISTORICAL_PATTERN = re.compile(r"historical_\d{8}\.txt")

REQUIRED_PROPERTY_FIELDS = ["EntityID", "DealName"]
REQUIRED_LINEITEM_FIELDS = ["LineItemId", "LineItemDescription", "redIQChartOfAccount", "IsExpenseAccount"]
REQUIRED_HISTORY_FIELDS = ["EntityId", "LineItemId", "Date", "IsAnnual", "Value"]


def read_tsv(file_bytes):
    text = io.TextIOWrapper(io.BytesIO(file_bytes), encoding="utf-8")
    reader = csv.DictReader(text, delimiter="\t")
    # Convert fieldnames to lowercase for case-insensitive matching
    fieldnames = {name.lower(): name for name in reader.fieldnames}
    rows = []
    for row in reader:
        # Create a new dict with lowercase keys
        new_row = {k.lower(): v for k, v in row.items()}
        rows.append(new_row)
    return fieldnames, rows


class BulkData:
    def __init__(self, property_rows, lineitem_rows, history_rows):
        self.properties = property_rows
        self.lineitems = lineitem_rows
        self.history = history_rows

    @classmethod
    def from_zip(cls, zip_path):
        with zipfile.ZipFile(zip_path, "r") as zf:
            prop_file = lineitem_file = hist_file = None
            logger.info(f"\nSearching for files matching patterns:")
            logger.info(f"- Property pattern: {PROPERTY_PATTERN.pattern}")
            logger.info(f"- Line items pattern: {LINEITEM_PATTERN.pattern}")
            logger.info(f"- Historical pattern: {HISTORICAL_PATTERN.pattern}")
            logger.info("\nFiles found in zip:")
            for name in zf.namelist():
                basename = os.path.basename(name)
                logger.info(f"- {basename}")
                if PROPERTY_PATTERN.fullmatch(basename):
                    prop_file = name
                    logger.info(f"  ✓ Matches property pattern")
                elif LINEITEM_PATTERN.fullmatch(basename):
                    lineitem_file = name
                    logger.info(f"  ✓ Matches line items pattern")
                elif HISTORICAL_PATTERN.fullmatch(basename):
                    hist_file = name
                    logger.info(f"  ✓ Matches historical pattern")
            if not (prop_file and lineitem_file and hist_file):
                missing = [
                    nm for nm, val in
                    {"property file": prop_file, "line items file": lineitem_file, "historical file": hist_file}.items()
                    if not val
                ]
                logger.error(f"\nMissing required files: {', '.join(missing)}")
                raise ValueError(f"Missing required files: {', '.join(missing)}")

            prop_fields, prop_rows = read_tsv(zf.read(prop_file))
            line_fields, line_rows = read_tsv(zf.read(lineitem_file))
            hist_fields, hist_rows = read_tsv(zf.read(hist_file))
            return cls(prop_rows, line_rows, hist_rows), (prop_fields, line_fields, hist_fields)

    def subset(self, num_properties):
        subset_props = self.properties[:num_properties]
        
        # Create case-insensitive sets for matching
        prop_ids_lower = {p["entityid"].lower() if p["entityid"] else "" for p in subset_props}
        
        # Use case-insensitive matching for history entries
        subset_history = [h for h in self.history 
                         if h["entityid"] and h["entityid"].lower() in prop_ids_lower]
        
        # Create case-insensitive set of line item IDs
        lineitem_ids_lower = {h["lineitemid"].lower() if h["lineitemid"] else "" for h in subset_history}
        
        # Use case-insensitive matching for line items
        subset_lineitems = [li for li in self.lineitems 
                           if li["lineitemid"] and li["lineitemid"].lower() in lineitem_ids_lower]
        
        return BulkData(subset_props, subset_lineitems, subset_history)

    def split(self, batch_size):
        """
        Split the data into multiple BulkData objects with batch_size properties each.
        Returns a list of BulkData objects.
        """
        if batch_size <= 0:
            raise ValueError("Batch size must be greater than 0")
        
        batches = []
        for i in range(0, len(self.properties), batch_size):
            batch_props = self.properties[i:i+batch_size]
            
            # Create case-insensitive sets for matching
            prop_ids_lower = {p["entityid"].lower() if p["entityid"] else "" for p in batch_props}
            
            # Use case-insensitive matching for history entries
            batch_history = [h for h in self.history 
                            if h["entityid"] and h["entityid"].lower() in prop_ids_lower]
            
            # Create case-insensitive set of line item IDs
            lineitem_ids_lower = {h["lineitemid"].lower() if h["lineitemid"] else "" for h in batch_history}
            
            # Use case-insensitive matching for line items
            batch_lineitems = [li for li in self.lineitems 
                              if li["lineitemid"] and li["lineitemid"].lower() in lineitem_ids_lower]
            
            batches.append(BulkData(batch_props, batch_lineitems, batch_history))
            
        return batches

    def write_zip(self, path, date_suffix="20200101"):
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
            def write_tsv(name, rows):
                if not rows:
                    return
                fieldnames = list(rows[0].keys())
                buffer = io.StringIO()
                writer = csv.DictWriter(buffer, fieldnames=fieldnames, delimiter="\t")
                writer.writeheader()
                writer.writerows(rows)
                zf.writestr(name, buffer.getvalue())

            write_tsv(f"property_{date_suffix}.txt", self.properties)
            write_tsv(f"lineItems_{date_suffix}.txt", self.lineitems)
            write_tsv(f"historical_{date_suffix}.txt", self.history)


def validate(data: BulkData, fields):
    prop_fields, line_fields, hist_fields = fields
    errors = defaultdict(list)
    stats = defaultdict(int)

    # Check required fields in file headers (case-insensitive)
    missing_prop_cols = [f for f in REQUIRED_PROPERTY_FIELDS if f.lower() not in prop_fields]
    if missing_prop_cols:
        errors["Missing Columns"].append(f"Property file is missing columns: {', '.join(missing_prop_cols)}")

    missing_line_cols = [f for f in REQUIRED_LINEITEM_FIELDS if f.lower() not in line_fields]
    if missing_line_cols:
        errors["Missing Columns"].append(f"Line items file is missing columns: {', '.join(missing_line_cols)}")

    missing_hist_cols = [f for f in REQUIRED_HISTORY_FIELDS if f.lower() not in hist_fields]
    if missing_hist_cols:
        errors["Missing Columns"].append(f"Historical file is missing columns: {', '.join(missing_hist_cols)}")

    # Validate properties
    ids = set()
    valid_props = set()
    valid_props_lower = set()  # Case-insensitive set for lookups
    
    for row in tqdm(data.properties, desc="Validating properties", leave=False):
        eid = row.get("entityid")  # Use lowercase key
        if not eid:
            errors["Missing Data"].append(f"Property row missing EntityID")
            continue
        if eid in ids:
            errors["Duplicate IDs"].append(f"EntityID {eid}")
            continue
        ids.add(eid)
        if not row.get("dealname"):  # Use lowercase key
            errors["Missing Data"].append(f"Property {eid} missing DealName")
            continue
        valid_props.add(eid)
        valid_props_lower.add(eid.lower())  # Add lowercase version for case-insensitive lookups
        stats["Valid Properties"] += 1

    # Validate line items
    line_ids = set()
    valid_lineitems = set()
    valid_lineitems_lower = set()  # Case-insensitive set for lookups
    
    for row in tqdm(data.lineitems, desc="Validating line items", leave=False):
        lid = row.get("lineitemid")  # Use lowercase key
        if not lid:
            errors["Missing Data"].append(f"Line item row missing LineItemId")
            continue
        if lid in line_ids:
            errors["Duplicate IDs"].append(f"LineItemId {lid}")
            continue
        line_ids.add(lid)
        if not row.get("lineitemdescription"):  # Use lowercase key
            errors["Missing Data"].append(f"Line item {lid} missing description")
            continue
        if not row.get("rediqchartofaccount"):  # Use lowercase key
            errors["Missing Data"].append(f"Line item {lid} missing redIQChartOfAccount")
            continue
        if row.get("isexpenseaccount") not in {"0", "1", 0, 1, True, False}:  # Use lowercase key
            errors["Invalid Data"].append(f"Line item {lid} invalid IsExpenseAccount {row.get('isexpenseaccount')}")
            continue
        valid_lineitems.add(lid)
        valid_lineitems_lower.add(lid.lower())  # Add lowercase version for case-insensitive lookups
        stats["Valid Line Items"] += 1

    # Validate history
    history_keys = set()
    valid_history = 0
    history_by_property = defaultdict(int)
    history_by_lineitem = defaultdict(int)
    
    for idx, row in enumerate(tqdm(data.history, desc="Validating history", leave=False), 1):
        eid = row.get("entityid")  # Use lowercase key
        lid = row.get("lineitemid")  # Use lowercase key
        date = row.get("date")  # Use lowercase key
        is_annual = row.get("isannual")  # Use lowercase key
        value = row.get("value")  # Use lowercase key
        
        # Track all history entries by property and line item, but only for valid properties/line items
        if eid and eid.lower() in valid_props_lower:
            history_by_property[eid] += 1
        if lid and lid.lower() in valid_lineitems_lower:
            history_by_lineitem[lid] += 1
        
        # Skip validation if missing required fields
        if not all([eid, lid, date, is_annual, value]):
            missing_fields = [f for f, v in [("EntityID", eid), ("LineItemId", lid), 
                                           ("Date", date), ("IsAnnual", is_annual), 
                                           ("Value", value)] if not v]
            errors["Missing Data"].append(f"History row {idx} missing: {', '.join(missing_fields)}")
            continue
            
        # Check for duplicates
        key = (eid.lower(), lid.lower(), date, is_annual)  # Use lowercase for case-insensitive duplicate check
        if key in history_keys:
            errors["Duplicate History"].append(f"Row {idx}: EntityID={eid}, LineItemId={lid}, Date={date}")
            continue
        history_keys.add(key)
        
        # Check references - case insensitive
        if eid.lower() not in valid_props_lower:
            errors["Invalid References"].append(f"History row {idx}: unknown EntityID {eid}")
            continue
        if lid.lower() not in valid_lineitems_lower:
            errors["Invalid References"].append(f"History row {idx}: unknown LineItemId {lid}")
            continue
            
        valid_history += 1
        stats["Valid History Entries"] += 1

    # Calculate relationship statistics
    props_with_history = sum(1 for count in history_by_property.values() if count > 0)
    lineitems_with_history = sum(1 for count in history_by_lineitem.values() if count > 0)
    
    stats["Properties with History"] = props_with_history
    stats["Line Items with History"] = lineitems_with_history
    stats["Properties without History"] = len(valid_props) - props_with_history
    stats["Line Items without History"] = len(valid_lineitems) - lineitems_with_history

    return errors, stats


def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate and manipulate redIQ bulk upload zip files")
    subparsers = parser.add_subparsers(dest="command")

    v = subparsers.add_parser("validate", help="Validate a bulk upload zip file")
    v.add_argument("zipfile")

    s = subparsers.add_parser("subset", help="Create subset zip from a bulk upload zip")
    s.add_argument("zipfile")
    s.add_argument("output_zip")
    s.add_argument("num_properties", type=int, help="Number of properties to include")
    
    sp = subparsers.add_parser("split", help="Split bulk upload zip into multiple files with specified batch size")
    sp.add_argument("zipfile")
    sp.add_argument("output_prefix", help="Prefix for output zip files (will be appended with _1.zip, _2.zip, etc.)")
    sp.add_argument("batch_size", type=int, help="Number of properties per output file")
    sp.add_argument("--output_dir", "-o", help="Directory to save output files (default: current directory)")

    args = parser.parse_args(argv)

    if args.command == "validate":
        print("Loading and validating data...")
        data, fields = BulkData.from_zip(args.zipfile)
        errs, stats = validate(data, fields)
        
        print("\nData Summary:")
        print("=" * 50)
        print(f"Total Properties: {len(data.properties)}")
        print(f"Total Line Items: {len(data.lineitems)}")
        print(f"Total History Entries: {len(data.history)}")
        print("\nValid Data:")
        print(f"- Valid Properties: {stats['Valid Properties']}")
        print(f"- Valid Line Items: {stats['Valid Line Items']}")
        print(f"- Valid History Entries: {stats['Valid History Entries']}")
        print("\nData Relationships:")
        print(f"- Properties with History: {stats['Properties with History']}")
        print(f"- Properties without History: {stats['Properties without History']}")
        print(f"- Line Items with History: {stats['Line Items with History']}")
        print(f"- Line Items without History: {stats['Line Items without History']}")
        
        if errs:
            print("\nValidation Issues:")
            print("=" * 50)
            for error_type, error_list in errs.items():
                print(f"\n{error_type} ({len(error_list)}):")
                # Only show first 5 examples of each error type
                for error in error_list[:5]:
                    print(f"  - {error}")
                if len(error_list) > 5:
                    print(f"  ... and {len(error_list) - 5} more")
            print("\n" + "=" * 50)
            print(f"\nTotal issues found: {sum(len(err_list) for err_list in errs.values())}")
            sys.exit(1)
        else:
            print("\nValidation successful!")
    elif args.command == "subset":
        data, _ = BulkData.from_zip(args.zipfile)
        subset = data.subset(args.num_properties)
        subset.write_zip(args.output_zip)
        print(f"Wrote subset with {len(subset.properties)} properties to {args.output_zip}")
    elif args.command == "split":
        print(f"Loading data from {args.zipfile}...")
        data, _ = BulkData.from_zip(args.zipfile)
        batches = data.split(args.batch_size)
        
        print(f"Splitting {len(data.properties)} properties into {len(batches)} batches of up to {args.batch_size} properties each")
        
        # Create output directory if specified and doesn't exist
        output_dir = args.output_dir if args.output_dir else ""
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
        
        for i, batch in enumerate(batches, 1):
            filename = f"{args.output_prefix}_{i}.zip"
            output_file = os.path.join(output_dir, filename) if output_dir else filename
            batch.write_zip(output_file)
            print(f"Batch {i}: Wrote {len(batch.properties)} properties to {output_file}")
            
        print(f"\nSuccessfully created {len(batches)} batch files")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
