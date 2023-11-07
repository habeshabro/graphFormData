import json
import os
import re

f2 = open('data.json')
query_objects = json.load(f2)
reports = {}
report_names = []

for index, qo in enumerate(query_objects):
    prefix = "DHIS2Q"
    if "RMH_FP_CYP" in qo['report_name']:
        prefix = "DHIS2Y"
    reports[f"{prefix}{index}"] =  {
        "name": f"{prefix}{index}",
        "type": "MRSGeneric",
        "requiredPrivilege": "app:reportsDHIS",
        "config": {
            "sqlPath": f"/var/www/bahmni_config/openmrs/apps/reports/sql/DHIS2/{qo['report_group']}/{qo['report_name']}.sql"
        }
    }
    report_names.append(f"{prefix}{index}")


with open("reports.json", 'w', encoding='utf-8') as f3:
    json.dump(report_names , f3, ensure_ascii=False, indent=4)
with open('to_append.json', 'w', encoding='utf-8') as f4:
    json.dump(reports, f4, ensure_ascii=False, indent=4)