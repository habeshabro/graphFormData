import mysql.connector
import os
from pathlib import Path
from os import walk
import re
import openpyxl
import simplejson as json
import numbers


#establishing the connection
conn = mysql.connector.connect(
   user='fetch-worker', password='VHwY7_D2X9^+gcV', host='192.168.56.10', database='openmrs')

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

# , "Manual Entries"
# spec_files = ["Reproductive, Maternal Newborn and Child Health", "Birth and Death Notification Forms",
#               "Disease Prevention and Control", "Health System related Registers", "Medical Services",
#               "NCD prevention, control and Mental Health", "NTD prevention and Control", "Manual Entries",
#               "DHIS Remaining", "DHIS2 EMR  - Not available on HMIS"]
excel_folder = "./generated"
filenames = next(os.walk(excel_folder))[1]


count = 0
pass_count = 0
fail_count = 0
overfetch = 0
test_result = {}
for file_name in filenames:
    print(file_name)
    filenames = next(walk(f"./generated/{file_name}"), (None, None, []))[2]
    for sql_file in filenames:
        full_path = f'./generated/{file_name}/{sql_file}'
        sql_doc = Path(full_path)
        if sql_doc.is_file() and full_path.endswith('.sql'):
            f = open(full_path)
            sql = f.read()
            sql = sql.replace("'#startDate#'", "date('2023-10-10')")
            sql = sql.replace("'#endDate#'", "date('2023-10-13')")
            pattern = "(?<=SELECT\s)([\S\s]*)(?=\sFROM)"
            replacement = '''DISTINCT person.person_id,
CONCAT(pn.given_name," ",pn.middle_name, " ", pn.family_name) '''
            sql2 = re.sub(pattern, replacement, sql)
            pattern = "FROM person"
            replacement = "FROM person JOIN person_name pn ON pn.person_id = person.person_id"
            sql2 = re.sub(pattern, replacement, sql2)
            if "MS_ICU_LOS.1." in sql:
                print(sql)
            try:
                cursor.execute(sql)
                raw_columns = re.findall("AS \"[^\"]+?\"", sql)
                columns = []
                for raw_col in raw_columns:
                    column = re.findall("\"[^\"]+?\"", raw_col)[0]
                    column = column.replace('"', "")
                    column = column.replace('/', "")
                    columns.append(column)
                resultss = cursor.fetchall()
                # cursor.execute(sql2)
                # names = cursor.fetchall()

                for results in resultss:
                    for num,result in enumerate(results):
                        # print(f'{columns[num]}:{result}')
                        count = count + 1

                        test_result[columns[num]] = {
                            "fetched value": result
                        }
                        if not isinstance(test_result[columns[num]]["fetched value"], numbers.Number):
                            test_result[columns[num]]["fetched value"] = 0

            except mysql.connector.errors.ProgrammingError as e:
                print(sql)
                print(e)
            # sql = sql.replace("'#startDate#'", "date('2023-06-12')")

# print(count)
# print(test_result)
tolerance = 0
filenames = next(walk("./CSVs"), (None, None, []))[2]

for excel_file in filenames:
    # just_name = excel_file.split(".")[0]
    full_path = f'./CSVs/{excel_file}'
    xlsx_file = Path(full_path)
    if xlsx_file.is_file() and full_path.endswith('.xlsx'):
        wb_obj = openpyxl.load_workbook(xlsx_file)
        sheet = wb_obj.active
        for row in range(1, 1000):
            flag_name = ""
            if sheet[f"A{row}"].value:
                flag_name = sheet[f"A{row}"].value.strip()
            if isinstance(sheet[f"H{row}"].value, int):
                if flag_name in test_result:
                    if "test_value" in test_result[flag_name]:
                        test_result[flag_name]["test_value"] = test_result[flag_name]["test_value"] + sheet[f"H{row}"].value
                    else:
                        test_result[flag_name]["test_value"] = sheet[f"H{row}"].value
                    if not isinstance(test_result[flag_name]["fetched value"], int):
                        print(flag_name)
                    test_result[flag_name]["test_status"] = abs(test_result[flag_name]["test_value"] - test_result[flag_name]["fetched value"]) <= tolerance
                    if test_result[flag_name]["fetched value"] > test_result[flag_name]["test_value"]:
                        overfetch = overfetch + 1
                    elif test_result[flag_name]["test_status"]:
                        pass_count = pass_count + 1
                    else:
                        fail_count = fail_count + 1

            elif sheet[f"G{row}"].value and isinstance(sheet[f"G{row}"].value, int):
                if flag_name in test_result:
                    if "test_value" in test_result[flag_name]:
                        test_result[flag_name]["test_value"] = test_result[flag_name]["test_value"] + sheet[
                            f"G{row}"].value
                    else:
                        test_result[flag_name]["test_value"] = sheet[f"G{row}"].value
                    test_result[flag_name]["test_status"] = abs(test_result[flag_name]["test_value"] - test_result[flag_name]["fetched value"]) < 2
                    if test_result[flag_name]["test_status"]:
                        pass_count = pass_count + 1
                    else:
                        fail_count = fail_count + 1
            elif sheet[f"F{row}"].value and isinstance(sheet[f"F{row}"].value, int):
                if flag_name.strip() in test_result:
                    if "test_value" in test_result[flag_name]:
                        test_result[flag_name]["test_value"] = test_result[flag_name]["test_value"] + sheet[
                            f"F{row}"].value
                    else:
                        test_result[flag_name]["test_value"] = sheet[f"F{row}"].value
                    test_result[flag_name]["test_status"] = abs(test_result[flag_name]["test_value"] - test_result[flag_name]["fetched value"]) < 2
                    if test_result[flag_name]["fetched value"] > test_result[flag_name]["test_value"]:
                        overfetch = overfetch + 1
                    elif test_result[flag_name]["test_status"]:
                        pass_count = pass_count + 1
                    else:
                        fail_count = fail_count + 1
                            #Closing the connection
conn.close()

# print(f"Passed: {pass_count} \nFailed: {fail_count}\nPass Rate: {(pass_count/(pass_count+fail_count)) * 100}%")
print(f"Passed: {pass_count} \nFailed: {fail_count}\n Overfetched: {overfetch}\n")
with open('test_result.json', 'w', encoding='utf-8') as f:
    json.dump(test_result, f, ensure_ascii=False, indent=4)
