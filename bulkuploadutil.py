# Utility to validate and manipulate redIQ bulk upload zip files
import argparse
import csv
import io
import re
import sys
import zipfile

PROPERTY_PATTERN = re.compile(r"property_\d{8}\.txt")
LINEITEM_PATTERN = re.compile(r"lineItems_\d{8}\.txt")
HISTORICAL_PATTERN = re.compile(r"historical_\d{8}\.txt")

REQUIRED_PROPERTY_FIELDS = ["EntityId", "DealName"]
REQUIRED_LINEITEM_FIELDS = ["LineItemId", "LineItemDescription", "redIQChartOfAccount", "IsExpenseAccount"]
REQUIRED_HISTORY_FIELDS = ["EntityId", "LineItemId", "Date", "IsAnnual", "Value"]


def read_tsv(file_bytes):
    text = io.TextIOWrapper(io.BytesIO(file_bytes), encoding="utf-8")
    reader = csv.DictReader(text, delimiter="\t")
    rows = list(reader)
    return reader.fieldnames, rows


class BulkData:
    def __init__(self, property_rows, lineitem_rows, history_rows):
        self.properties = property_rows
        self.lineitems = lineitem_rows
        self.history = history_rows

    @classmethod
    def from_zip(cls, zip_path):
        with zipfile.ZipFile(zip_path, "r") as zf:
            prop_file = lineitem_file = hist_file = None
            for name in zf.namelist():
                if PROPERTY_PATTERN.fullmatch(name):
                    prop_file = name
                elif LINEITEM_PATTERN.fullmatch(name):
                    lineitem_file = name
                elif HISTORICAL_PATTERN.fullmatch(name):
                    hist_file = name
            if not (prop_file and lineitem_file and hist_file):
                missing = [
                    nm for nm, val in
                    {"property file": prop_file, "line items file": lineitem_file, "historical file": hist_file}.items()
                    if not val
                ]
                raise ValueError(f"Missing required files: {', '.join(missing)}")

            prop_fields, prop_rows = read_tsv(zf.read(prop_file))
            line_fields, line_rows = read_tsv(zf.read(lineitem_file))
            hist_fields, hist_rows = read_tsv(zf.read(hist_file))
            return cls(prop_rows, line_rows, hist_rows), (prop_fields, line_fields, hist_fields)

    def subset(self, num_properties):
        subset_props = self.properties[:num_properties]
        prop_ids = {p["EntityId"] for p in subset_props}
        subset_history = [h for h in self.history if h["EntityId"] in prop_ids]
        lineitem_ids = {h["LineItemId"] for h in subset_history}
        subset_lineitems = [li for li in self.lineitems if li["LineItemId"] in lineitem_ids]
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
    errors = []

    missing_prop = [f for f in REQUIRED_PROPERTY_FIELDS if f not in prop_fields]
    if missing_prop:
        errors.append(f"Property file missing fields: {', '.join(missing_prop)}")

    missing_line = [f for f in REQUIRED_LINEITEM_FIELDS if f not in line_fields]
    if missing_line:
        errors.append(f"Line items file missing fields: {', '.join(missing_line)}")

    missing_hist = [f for f in REQUIRED_HISTORY_FIELDS if f not in hist_fields]
    if missing_hist:
        errors.append(f"Historical file missing fields: {', '.join(missing_hist)}")

    ids = set()
    for row in data.properties:
        eid = row.get("EntityId")
        if not eid:
            errors.append("Property row missing EntityId")
        elif eid in ids:
            errors.append(f"Duplicate EntityId {eid}")
        ids.add(eid)
        if not row.get("DealName"):
            errors.append(f"Property {eid} missing DealName")

    line_ids = set()
    for row in data.lineitems:
        lid = row.get("LineItemId")
        if not lid:
            errors.append("Line item row missing LineItemId")
        elif lid in line_ids:
            errors.append(f"Duplicate LineItemId {lid}")
        line_ids.add(lid)
        if not row.get("LineItemDescription"):
            errors.append(f"Line item {lid} missing description")
        if not row.get("redIQChartOfAccount"):
            errors.append(f"Line item {lid} missing redIQChartOfAccount")
        if row.get("IsExpenseAccount") not in {"0", "1", 0, 1, True, False}:
            errors.append(f"Line item {lid} invalid IsExpenseAccount {row.get('IsExpenseAccount')}")

    history_keys = set()
    for idx, row in enumerate(data.history, 1):
        eid = row.get("EntityId")
        lid = row.get("LineItemId")
        key = (eid, lid, row.get("Date"), row.get("IsAnnual"))
        if key in history_keys:
            errors.append(f"Duplicate history row {key}")
        history_keys.add(key)
        if eid not in ids:
            errors.append(f"History row {idx} references unknown EntityId {eid}")
        if lid not in line_ids:
            errors.append(f"History row {idx} references unknown LineItemId {lid}")
        if not row.get("Value"):
            errors.append(f"History row {key} missing Value")
    return errors


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
        data, fields = BulkData.from_zip(args.zipfile)
        errs = validate(data, fields)
        if errs:
            print("Validation failed:")
            for e in errs:
                print(" -", e)
            sys.exit(1)
        else:
            print("Validation successful.\nProperties: {}\nLine items: {}\nHistory rows: {}".format(
                len(data.properties), len(data.lineitems), len(data.history)))
    elif args.command == "subset":
        data, _ = BulkData.from_zip(args.zipfile)
        subset = data.subset(args.num_properties)
        subset.write_zip(args.output_zip)
        print(f"Wrote subset with {len(subset.properties)} properties to {args.output_zip}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
