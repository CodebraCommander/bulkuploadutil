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
        prop_ids = {p["entityid"] for p in subset_props}
        subset_history = [h for h in self.history if h["entityid"] in prop_ids]
        lineitem_ids = {h["lineitemid"] for h in subset_history}
        subset_lineitems = [li for li in self.lineitems if li["lineitemid"] in lineitem_ids]
        return BulkData(subset_props, subset_lineitems, subset_history)

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
    prop_case_variants = defaultdict(set)  # Track case variants of EntityIDs
    
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
        # Store the original case of the EntityID
        prop_case_variants[eid.lower()].add(eid)
        stats["Valid Properties"] += 1

    # Validate line items
    line_ids = set()
    valid_lineitems = set()
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
        stats["Valid Line Items"] += 1

    # Validate history
    history_keys = set()
    valid_history = 0
    history_by_property = defaultdict(int)
    history_by_lineitem = defaultdict(int)
    hist_case_variants = defaultdict(set)  # Track case variants in history file
    
    for idx, row in enumerate(tqdm(data.history, desc="Validating history", leave=False), 1):
        eid = row.get("entityid")  # Use lowercase key
        lid = row.get("lineitemid")  # Use lowercase key
        date = row.get("date")  # Use lowercase key
        is_annual = row.get("isannual")  # Use lowercase key
        value = row.get("value")  # Use lowercase key
        
        # Track case variants in history file
        if eid:
            hist_case_variants[eid.lower()].add(eid)
        
        # Track all history entries by property and line item
        if eid:
            history_by_property[eid] += 1
        if lid:
            history_by_lineitem[lid] += 1
        
        # Skip validation if missing required fields
        if not all([eid, lid, date, is_annual, value]):
            missing_fields = [f for f, v in [("EntityID", eid), ("LineItemId", lid), 
                                           ("Date", date), ("IsAnnual", is_annual), 
                                           ("Value", value)] if not v]
            errors["Missing Data"].append(f"History row {idx} missing: {', '.join(missing_fields)}")
            continue
            
        # Check for duplicates
        key = (eid, lid, date, is_annual)
        if key in history_keys:
            errors["Duplicate History"].append(f"Row {idx}: EntityID={eid}, LineItemId={lid}, Date={date}")
            continue
        history_keys.add(key)
        
        # Check references
        if eid not in valid_props:
            errors["Invalid References"].append(f"History row {idx}: unknown EntityID {eid}")
            continue
        if lid not in valid_lineitems:
            errors["Invalid References"].append(f"History row {idx}: unknown LineItemId {lid}")
            continue
            
        valid_history += 1
        stats["Valid History Entries"] += 1

    # Check for case sensitivity issues in EntityID references
    case_sensitivity_issues = []
    for eid_lower, prop_variants in prop_case_variants.items():
        hist_variants = hist_case_variants.get(eid_lower, set())
        if hist_variants and prop_variants != hist_variants:
            case_sensitivity_issues.append(
                f"EntityID case mismatch: Property file has {prop_variants}, History file has {hist_variants}"
            )
    
    if case_sensitivity_issues:
        errors["Case Sensitivity Issues"] = case_sensitivity_issues

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
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
