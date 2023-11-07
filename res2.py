import json
import os
import re

f2 = open('data.json')
query_objects = json.load(f2)
reports = {}
report_names = []

for index, qo in enumerate(query_objects):
    reports[qo['report_name']] =  {
        "name":qo['report_name'],
        "type": "MRSGeneric",
        "requiredPrivilege": "app:reports",
        "config": {
            "sqlPath": f"/var/www/bahmni_config/openmrs/apps/reports/sql/generated/{qo['report_group']}/{qo['report_name']}.sql"
        }
    }

with open('to_append2.json', 'w', encoding='utf-8') as f4:
    json.dump(reports, f4, ensure_ascii=False, indent=4)