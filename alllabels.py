import csv
import time
from googleads import ad_manager # Import the library

# Initialize a client object, by default uses the credentials in ~/googleads.yaml
client = ad_manager.AdManagerClient.LoadFromStorage()

# Assume you have set up the GAM API client (client)
API_VERSION = "v202511"  # change if you use a different version
PAGE_SIZE = 500 # GAM max is often 500

label_service = client.GetService('LabelService', version=API_VERSION)

# Create a query to select all companies
statement = (ad_manager.StatementBuilder()
             #.Where("id = '259944'")
             .Limit(PAGE_SIZE))


with open("labels.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["id", "types", "name", "adCategory", "isActive"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    # Retrieve companies
    while True:
        try:
            page = label_service.getLabelsByStatement(statement.ToStatement())
        except Exception as e:
            time.sleep(2)
            page = label_service.getLabelsByStatement(statement.ToStatement())
        results = page['results'] if 'results' in page else []
        if not results:
            break
        for label in results:    
        #    applied = company['appliedLabels'] if 'appliedLabels' in company else []
        #    label_ids = [
        #        str(x.labelId)
        #        for x in applied
        #        if getattr(x, "labelId", None) is not None and not getattr(x,"isNegated", False)
        #    ]
        #    labelids_str = ";".join(label_ids)
            writer.writerow({
                #"id": str(label["id"]),
                #"type": label["type"],
                #"name": label["name"],
                #"adCategory": label["adCategory"],
                #"description": label["description"],
                #"isActive": label["isActive"]
                "id": str(getattr(label,"id","")),
                "types": str(getattr(label,"types","")),
                "name": str(getattr(label,"name","")),
                "adCategory": str(getattr(label,"adCategory","")),
                "isActive": str(getattr(label,"isActive",""))
            })
            #print('Label ID: "%s", Type: "%s", Name: "%s", AdCategory: "%s", Description: "%s", isActive: "%s"' %
            #      (label['id'], label['type'], label['name'], label['adCategory'], label['description'], label['isActive']))
            print(
            "Label ID: %s, Types: %s, Name: %s, AdCategory: %s, isActive: %s"
                % (
                    getattr(label,"id",""),
                    getattr(label,"types",""),
                    getattr(label,"name",""),
                    getattr(label,"adCategory",""),
                    getattr(label,"isActive","")
                ),)
        statement.offset += PAGE_SIZE

    print('\nNumber of labels found: %s' %page['totalResultSetSize'])