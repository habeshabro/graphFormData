import re
import openpyxl
from pathlib import Path
from os import walk
import json

f = open('./combo/combo.json')
f2 = open('data element/data_element.json')
combo = json.load(f)
data_elements = json.load(f2)


filenames = next(walk("./excel_sheets"), (None, None, []))[2]


def name_search_json_object(json_objects, property_name, property_value):
    results = []
    targ_names = []
    for json_object in json_objects:
        targ_name = json_object[property_name].replace(" ", "")
        if (targ_name == 'Secondtrimester(>=12Weeks)'):
            targ_name = 'SecondTrimester(≥12-28weeks)'
        targ_names.append(targ_name)
        property_value = property_value.replace(" ", "")
        if property_name in json_object and (property_value in targ_name or targ_name in property_value):
            results.append(json_object)

    if results.__len__() == 0:
        for json_object in json_objects:
            targ_name = json_object[property_name].replace(" ", "")
            targ_names.append(targ_name)
            property_value = property_value.replace(" ", "")
            if property_name in json_object and (property_value in targ_name or targ_name in property_value):
                results.append(json_object)

    if results.__len__() == 0:
        print(property_value)
        print(targ_names)

    return results


def str_search_json_object(json_objects, property_name, property_value, name):
    results = []
    for json_object in json_objects:
        if property_name in json_object and json_object[property_name] == property_value:
            results.append(json_object)
        elif property_name in json_object and json_object[property_name] + "s" == property_value:
            results.append(json_object)
    split_str = property_value.split(". ")
    if results.__len__() == 0 and split_str.__len__() > 0:
        return search_json_object(json_objects, property_name, split_str[0], True, name)
    return results


def search_json_object(json_objects, property_name, property_value, from_str, name):
    results = []
    for json_object in json_objects:
        if property_name in json_object and json_object[property_name] == property_value:
            results.append(json_object)
        if from_str and (name.replace(" ","") in json_object["name"].replace(" ","") or ("formName" in json_object and name.replace(" ","") in json_object["formName"].replace(" ",""))):
            results.append(json_object)
    if results.__len__() == 0:
        for json_object in json_objects:
            if property_name in json_object and json_object[property_name] == property_value + ".1":
                results.append(json_object)
    if results.__len__() == 0:
        for json_object in json_objects:
            if "attributeValues" in json_object:
                if json_object["attributeValues"].__len__() > 0 and json_object["attributeValues"][0]["value"] == property_value:
                    results.append(json_object)

    if results.__len__() == 0:
        print(property_value)
    if "TB_DR_CTX." in property_name:
        print("aaaa")
        print(results)

    return results


completed_map = []

for excel_file in filenames:
    just_name = excel_file.split(".")[0]
    full_path = f'./excel_sheets/{excel_file}'
    xlsx_file = Path(full_path)
    if xlsx_file.is_file() and full_path.endswith('.xlsx'):
        wb_obj = openpyxl.load_workbook(xlsx_file)
        sheet = wb_obj.active
        jump = 0
        for row in range(1, 1000):
            curr_code = sheet[f"A{row}"].value
            nxt_code = sheet[f"A{row + 1}"].value

            if curr_code:
                if curr_code + ". 1" == nxt_code:
                    element_code = curr_code
                    resp = search_json_object(data_elements["dataElements"], "code", element_code, True, sheet[f"B{row}"].value)
                    data_element_code = resp[0]["id"]
                    catagory_combo = resp[0]["categoryCombo"]
                    resp2 = search_json_object(combo["categoryOptionCombos"], "categoryCombo", catagory_combo, False, "xxxxxxx")

                    sub_elements = []
                    for sub_row in range(1,40):
                        if sheet[f"A{row + sub_row}"].value == sheet[f"A{row}"].value + f". {sub_row}":
                            sub_elements.append({
                                "code": sheet[f"A{row + sub_row}"].value,
                                "name": sheet[f"B{row + sub_row}"].value
                            })
                    for response in resp:
                        if sub_elements.__len__() != resp2.__len__() or name_search_json_object(resp2, "name", sub_elements[0]["name"]).__len__() == 0:
                            data_element_code = response["id"]
                            catagory_combo = response["categoryCombo"]
                            resp2 = search_json_object(combo["categoryOptionCombos"], "categoryCombo", catagory_combo,
                                                       False, "xxxxxxx")

                    for sub_element in sub_elements:
                        resp3 = name_search_json_object(resp2, "name", sub_element["name"])
                        if resp3.__len__() == 0:
                            print(resp.__len__())
                            print(sub_elements)
                        else:
                            completed_map.append({
                                "actual_code": sub_element["code"],
                                "data_element": data_element_code,
                                "catagory_combo": resp3[0]["id"]
                            })
                elif curr_code.endswith("."):
                    actual_code = sheet[f"A{row}"].value.rstrip('.')
                    resp = search_json_object(data_elements["dataElements"], "code", actual_code, True, sheet[f"B{row}"].value)
                    if resp.__len__() == 0:
                        print(actual_code)
                    data_element_code = resp[0]["id"]
                    catagory_combo = resp[0]["categoryCombo"]
                    resp2 = search_json_object(combo["categoryOptionCombos"], "categoryCombo", catagory_combo, False, "xxxxxxx")
                    completed_map.append({
                        "actual_code": sheet[f"A{row}"].value,
                        "data_element": data_element_code,
                        "catagory_combo": resp2[0]["id"]

                    })
#
#                 actual_code = sheet[f"A{row}"].value.rstrip('.')
#                 resp = str_search_json_object(data_elements["dataElements"], "code", actual_code)
#                 if resp.__len__() > 0:
#                     data_element_code = resp[0]["id"]
#                     catagory_combo = resp[0]["categoryCombo"]
#                     resp2 = search_json_object(combo["categoryOptionCombos"], "categoryCombo", catagory_combo, False)
#                     if resp2.__len__() == 1:
#                         to_be_added = {
#                             "actual_code": sheet[f"A{row}"].value,
#                             "data_element": data_element_code,
#                             "catagory_combo": resp2[0]["id"]
#                         }
#                         completed_map.append(to_be_added)
#                     else:
#                         for count, actual_element in enumerate(resp2):
#                             ind = count + 1
#
#                             to_be_added = {
#                                 "actual_code": actual_code.split(". ")[0]+f". {ind}",
#                                 "data_element": data_element_code,
#                                 "catagory_combo": actual_element["id"]
#                             }
#                             completed_map.append(to_be_added)
#
print(completed_map.__len__())
with open('map.json', 'w', encoding='utf-8') as f:
    json.dump(completed_map, f, ensure_ascii=False, indent=4)






