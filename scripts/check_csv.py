import sys
import csv
from datetime import datetime

CSV_FILE = "src/main/resources/production.csv"

EXPECTED_FIELDS = [0, 1, 2, 3, 4, 5, 6, 7]
FIELD_NAMES = {
    0: "InvoiceNo",
    1: "StockCode",
    2: "Description",
    3: "Quantity",
    4: "InvoiceDate",
    5: "UnitPrice",
    6: "CustomerID",
    7: "Country",
}

DATE_FORMAT = "%m/%d/%Y %H:%M"

def parse_row(row):
    try:
        for idx in EXPECTED_FIELDS:
            _ = row[idx]
        return {
            "invoiceNo": row[0],
            "quantity": int(row[3]),
            "invoiceDate": row[4],
            "unitPrice": float(row[5]),
            "customerID": row[6],
            "country": row[7],
        }
    except Exception as e:
        return None

bad_lines = []
empty_counts = {FIELD_NAMES[idx]: 0 for idx in EXPECTED_FIELDS}
invalid_date_count = 0

with open(CSV_FILE, encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader, None)
    for lineno, row in enumerate(reader, start=2):
        for idx in EXPECTED_FIELDS:
            if idx >= len(row) or row[idx].strip() == "":
                empty_counts[FIELD_NAMES[idx]] += 1
        parsed = parse_row(row)
        if parsed is None:
            print(f"⚠️  Bad line {lineno}: {row}")
            bad_lines.append((lineno, row))
        else:
            date_str = parsed["invoiceDate"]
            try:
                datetime.strptime(date_str, DATE_FORMAT)
            except Exception:
                invalid_date_count += 1

print(f"\nFound {len(bad_lines)} bad lines.")
print("Empty field counts:")
for field, count in empty_counts.items():
    print(f"  {field}: {count}")
print(f"Number of dates not matching format {DATE_FORMAT}: {invalid_date_count}")
