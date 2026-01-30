import os
print("SCRIPT:", os.path.abspath(__file__), flush=True)
print("CWD:", os.getcwd(), flush=True)
print("OUTPUT ABS:", os.path.abspath("companies.csv"))

out_path = "/mnt/c/temp/gam_exports/companies.csv"
print("Writing to:", out_path, flush=True)

import csv
import time
from googleads import ad_manager # Import the library
from zeep.helpers import serialize_object

# Initialize a client object, by default uses the credentials in ~/googleads.yaml
client = ad_manager.AdManagerClient.LoadFromStorage()

# Assume you have set up the GAM API client (client)
API_VERSION = "v202511"  # change if you use a different version
PAGE_SIZE = 500 # GAM max is often 500

# Initialize GAM API services
company_service = client.GetService('CompanyService', version=API_VERSION)
label_service = client.GetService('LabelService', version=API_VERSION)

def build_label_map():
    # Return dict: {label_id(str):label_name(str)}
    label_map = {}
    statement = (ad_manager.StatementBuilder().Limit(PAGE_SIZE))
    while True:
        page = label_service.getLabelsByStatement(statement.ToStatement())
        page = serialize_object(page)
        results = page.get("results",[])
        if not results:
            break
            
        for lbl in results:
            #lbl is now a dict
            lid = str(lbl.get("id", ""))
            name = lbl.get("name", "")
            if lid:
                label_map[lid] = name
        statement.offset = (statement.offset + PAGE_SIZE)
    return label_map

label_map = build_label_map()

# --- Companies export ---
# Create a query to select all companies
statement = (ad_manager.StatementBuilder()
             #.Where("name LIKE :name")
             #.WithBindVariable("name", "%CIBC%")
             .Limit(PAGE_SIZE))

print(f"Writing CSV to: {out_path}", flush=True)
with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["id", "name", "type", "labelIds", "labelNames"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    processed = 0
    total = None
    
    # Retrieve companies
    while True:
        try:
            page = company_service.getCompaniesByStatement(statement.ToStatement())
            page = serialize_object(page)
            if total is None:
                total = page.get("totalResultSetSize")
                print(f"Found {total} companies to process.", flush=True)
        except Exception as e:
            time.sleep(2)
            page = company_service.getCompaniesByStatement(statement.ToStatement())
        
        page = serialize_object(page)
        results = page.get("results", [])
        if not results:
            break
        for company in results:
            processed += 1
            print (f"Processed {processed} {total}", end="\r", flush=True)
            applied = company.get("appliedLabels", []) or [] 

            # appliedLabels is a list of dicts like: {"labelId": 123, "isNegated": False}
            label_ids = [
                str(x.get("labelId"))
                for x in applied
                if x.get("labelId") is not None and not x.get("isNegated", False)
            ]

            label_names = [label_map.get(lid, "") for lid in label_ids]

            #remove blanks + de-dedupe while preserving order
            seen = set()
            label_names = [n for n in label_names if n and not (n in seen or seen.add(n))]

            writer.writerow({
                "id": str(company.get("id", "")),
                "name": company.get("name", ""),
                "type": company.get("type", ""),
                "labelIds": ";".join(label_ids),
                "labelNames": ";".join(label_names)
            })
        statement.offset = (statement.offset + PAGE_SIZE)
            
print(f"\nDone. Parsed {processed} companies. Number of companies found: {total}", flush=True)
print("\nFinished writing.", flush=True)
print("Exists?", os.path.exists(out_path),flush=True)
print("Size:", os.path.getsize(out_path) if os.path.exists(out_path) else "N/A", flush=True)