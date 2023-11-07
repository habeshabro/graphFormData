import re
import openpyxl
from pathlib import Path
from os import walk
import json


def parse_condition(raw_condition):
    pure = raw_condition
    proto_condition = {
        "more_joins": "",
        "more_where": ""
    }
    raw_condition = raw_condition.replace("Count number of ", "")
    raw_condition = raw_condition.replace("count number of ", "")
    raw_condition = raw_condition.replace("count the number of ", "")
    raw_condition = raw_condition.replace("Count all ", "")
    raw_condition = raw_condition.replace("count all ", "")
    raw_condition = raw_condition.replace("forms filled", "")
    raw_condition = raw_condition.replace('"Gender" IS "Female"', f"person.gender = 'F'")
    raw_condition = raw_condition.replace('"Gender" IS "Male"', f"person.gender = 'M'")
    condition_components = {
        "type_1": [],
        "type_2": [],
        "type_3": [],
        "query_struct" : ""
    }
    counter = 0
    if "VisitType" in raw_condition:
        proto_condition["more_joins"] = proto_condition["more_joins"] + "JOIN encounter ON encounter.visit_id = v.visit_id\nJOIN location ON location.location_id = encounter.location_id"
        searched = re.findall('(\"[^\"]+?\" IS ["]*VisitType["]*|["]*VisitType["]* IS \"[^\"]+?\")', raw_condition)
        first = True
        where_string = ""
        for res in searched:
            val = re.findall('"(?!VisitType| IS).+?"', res)[0]
            if first:
                where_string = where_string + f"\nAND (location.name LIKE {val}"
                first = False
            else:
                where_string = where_string + f" OR location.name LIKE {val}"
            raw_condition = raw_condition.replace(res, "location.retired=0")
        where_string = where_string + ")"
        proto_condition["more_joins"] = proto_condition["more_joins"] + where_string

    if re.search('(\"[^\"]+?\" IS ["]*[Dd]iagnosis["]*|["]*[Dd]iagnosis["]* IS \"[^\"]+?\")', raw_condition):
        diag_conditions = re.findall('(\"[^\"]+?\" IS ["]*[Dd]iagnosis["]*|["]*[Dd]iagnosis["]* IS \"[^\"]+?\")', raw_condition)

        for res in diag_conditions:
            concept = re.findall('"(?![dD]iagnosis| IS).+?"', res)[0]
            qry = f"""\nLEFT JOIN obs diagnosis{counter} ON person.person_id = diagnosis{counter}.person_id AND date(diagnosis{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND diagnosis{counter}.voided = 0
                    AND diagnosis{counter}.value_coded IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {concept})"""
            raw_condition = raw_condition.replace(res, f"diagnosis{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\" IS \"Drug\"", raw_condition):
        drug_conditions = re.findall("\"[^\"]+?\" IS \"Drug\"", raw_condition)
        for res in drug_conditions:
            concept = re.findall("\"[^\"]+?\"", res)[0]
            qry = f"""\nLEFT JOIN orders drug_order{counter} ON person.person_id = drug_order{counter}.patient_id AND date(drug_order{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND drug_order{counter}.voided = 0
                    AND drug_order{counter}.order_type_id IN (SELECT order_type_id from order_type WHERE name = 'Drug Order')
                    AND drug_order{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {concept}) """
            raw_condition = raw_condition.replace(res, f"drug_order{counter}.order_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\" IS \"Procedure\"", raw_condition):
        drug_conditions = re.findall("\"[^\"]+?\" IS \"Procedure\"", raw_condition)
        for res in drug_conditions:
            concept = re.findall("\"[^\"]+?\"", res)[0]
            qry = f"""\nLEFT JOIN orders procedure_order{counter} ON person.person_id = procedure_order{counter}.patient_id AND date(procedure_order{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND procedure_order{counter}.voided = 0
                    AND procedure_order{counter}.order_type_id IN (SELECT order_type_id from order_type WHERE name = 'Procedure Order')
                    AND procedure_order{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {concept}) """
            raw_condition = raw_condition.replace(res, f"procedure_order{counter}.order_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\" [><=]+ [0-9.]+", raw_condition):
        form_under_conditions = re.findall("\"[^\"]+?\" [><=]+ [0-9.]+? from form \"[^\"]+?\"", raw_condition)
        for res in form_under_conditions:
            concept = re.findall("\"[^\"]+?\"", res)[0]
            value = re.findall("[0-9]+", res)[0]
            operator = re.findall("[><=]+", res)[0]
            qry = f"""\nLEFT JOIN obs eq_obs{counter} ON person.person_id = eq_obs{counter}.person_id AND date(eq_obs{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND eq_obs{counter}.voided = 0
                        AND eq_obs{counter}.value_numeric {operator} {value}
                        AND eq_obs{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {concept})"""
            raw_condition = raw_condition.replace(res, f"eq_obs{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

        form_under_conditions = re.findall("\"[^\"]+?\" [><=]+ [0-9.]+", raw_condition)
        for res in form_under_conditions:
            concept = re.findall("\"[^\"]+?\"", res)[0]
            value = re.findall("[0-9.]+", res)[0]
            operator = re.findall("[><=]+", res)[0]
            qry = f"""\nLEFT JOIN obs eq_obs{counter} ON person.person_id = eq_obs{counter}.person_id AND date(eq_obs{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND eq_obs{counter}.voided = 0
                        AND eq_obs{counter}.value_numeric {operator} {value}
                        AND eq_obs{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {concept})
                        """
            raw_condition = raw_condition.replace(res, f"eq_obs{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\" under \"[^\"]+?\" from form \"[^\"]+?\"", raw_condition):
        form_under_conditions = re.findall("\"[^\"]+?\" under \"[^\"]+?\" from form \"[^\"]+?\"", raw_condition)

        for res in form_under_conditions:
            ans_name = re.findall("\"[^\"]+?\"", res)[0]
            concept = re.findall("\"[^\"]+?\"", res)[1]
            form_name = re.findall("\"[^\"]+?\"", res)[2]
            qry = f"""\nLEFT JOIN obs form_under_obs{counter} ON person.person_id = form_under_obs{counter}.person_id AND date(form_under_obs{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND form_under_obs{counter}.voided = 0
                            AND form_under_obs{counter}.obs_group_id IN (SELECT obs.obs_id from obs JOIN concept_name cn ON obs.concept_id = cn.concept_id WHERE concept_name_type = "FULLY_SPECIFIED" AND cn.name LIKE {form_name})
                            AND form_under_obs{counter}.value_coded IN (SELECT concept_id from concept_name cn WHERE cn.concept_name_type = "FULLY_SPECIFIED" AND cn.name LIKE {ans_name})
                            AND form_under_obs{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE  {concept})
                            """
            raw_condition = raw_condition.replace(res, f"form_under_obs{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\" from form \"[^\"]+?\"", raw_condition):
        form_conditions = re.findall("\"[^\"]+?\" from form \"[^\"]+?\"", raw_condition)
        for res in form_conditions:
            concept = re.findall("\"[^\"]+?\"", res)[0]
            form_name = re.findall("\"[^\"]+?\"", res)[1]
            qry = f"""\nLEFT JOIN obs form_under_obs{counter} ON person.person_id = form_under_obs{counter}.person_id AND date(form_under_obs{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND form_under_obs{counter}.voided = 0
                                        AND form_under_obs{counter}.obs_group_id IN (SELECT obs.obs_id from obs JOIN concept_name cn ON obs.concept_id = cn.concept_id WHERE concept_name_type = "FULLY_SPECIFIED" AND cn.name LIKE {form_name})
                                        AND form_under_obs{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE  {concept})
                                        """
            raw_condition = raw_condition.replace(res, f"form_under_obs{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\" under \"[^\"]+?\"", raw_condition):
        simple_conditions = re.findall("\"[^\"]+?\" under \"[^\"]+?\"", raw_condition)
        for res in simple_conditions:
            ans_name = re.findall("\"[^\"]+?\"", res)[0]
            concept = re.findall("\"[^\"]+?\"", res)[1]
            qry = f"""\nLEFT JOIN obs form_under_obs{counter} ON person.person_id = form_under_obs{counter}.person_id AND date(form_under_obs{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND form_under_obs{counter}.voided = 0
                        AND form_under_obs{counter}.value_coded IN (SELECT concept_id from concept_name cn WHERE cn.concept_name_type = "FULLY_SPECIFIED" AND cn.name LIKE {ans_name})
                        AND form_under_obs{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE  {concept})"""
            raw_condition = raw_condition.replace(res, f"form_under_obs{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    if re.search("\"[^\"]+?\"", raw_condition):
        simple_conditions = re.findall("\"[^\"]+?\"", raw_condition)
        for res in simple_conditions:
            qry = f"""\nLEFT JOIN obs form_under_obs{counter} ON person.person_id = form_under_obs{counter}.person_id AND date(form_under_obs{counter}.date_created) between date(v.date_created) and if(v.date_stopped, date(v.date_stopped), current_date()) AND form_under_obs{counter}.voided = 0
                        AND form_under_obs{counter}.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {res})"""
            raw_condition = raw_condition.replace(res, f"form_under_obs{counter}.obs_id IS NOT NULL")
            proto_condition["more_joins"] = proto_condition["more_joins"] + qry
            counter = counter + 1

    age_cond = ""
    if re.search("[Aa]ge [><=]+ [0-9.]+ ([Yy]ear|[Mm]onth|[Dd]ay)", raw_condition):
        age_condition = re.findall("[Aa]ge [><=]+ [0-9.]+ [Yy]ear", raw_condition) + re.findall("[Aa]ge [><=]+ [0-9.]+ [Mm]onth", raw_condition)
        for res in age_condition:
            value = re.findall("[0-9.]+", res)[0]
            comp_op = re.findall("[><=]+", res)[0]
            unit = re.findall("([Yy]ear|[Mm]onth|[Dd]ay)", res)[0]

            qry = f"\nAND TIMESTAMPDIFF({unit.upper()}, person.birthdate, v.date_started) {comp_op} {value}"
            raw_condition = raw_condition.replace(res, f"person.voided = 0") + qry


    # f"TIMESTAMPDIFF(YEAR, person.birthdate, encounter.encounter_datetime) <=
    # if " under " in raw_condition:
    #     print(pure)
    #     print(raw_condition)
    raw_condition = raw_condition.replace("Count sum of", "")
    raw_condition = raw_condition.replace("Count ", "")
    raw_condition = raw_condition.replace("count ", "")
    raw_condition = raw_condition.replace(" filled", "")

    proto_condition["more_where"] = raw_condition
    # print(raw_condition)
    return proto_condition


def append_singular(sh, ro, lab, just_nam, rep, flag):
    lims = [set_limitation_for_single(sh, ro)]
    labs = [stringy(lab)]
    s_d = ""
    sdb = ""
    if flag and sh[f"C{ro}"].value:
        s_d = sh[f"C{ro}"].value
        sdb = get_Disaggregator(sh, ro)
        if s_d == sdb:
            s_d = '"%"'

    rep.append({
        "single_dissagregator": s_d,
        "sd_by": sdb,
        "report_name": lab.rstrip('.'),
        "report_group": just_nam,
        "primary_counted_object": "person",
        "labels": labs,
        "categorized_by": {
            "age": [],
            "gender": False,
            "other": []
        },
        "limiting_conditions": lims
    })


def find_aggregation(val, agg_data, d_by_age, a_by, d_by):
    return_object = {
                            "age": [],
                            "gender": False,
                            "other": []
                    }

    if re.search("disaggregate.+?sex", a_by.lower()):
        return_object["gender"] = True
    if re.search("disaggregate.+?age", a_by.lower()):
        return_object["age"] = val
    elif re.search("\"[^\"]+?\"", a_by):
        form_d = ""
        if re.findall("\"[^\"]+?\"", a_by).__len__() > 1:
            form_d = re.findall("\"[^\"]+?\"", a_by)[1]
        return_object["other"] = [{
            "table": "obs",
            "limiting_conditions": [{
                "table": "concept_name",
                "type": "=",
                "col_name": "name",
                "value": re.findall("\"[^\"]+?\"", a_by)[0],
                "form_d": form_d
            }],
            "values": val
        }]


    if d_by:
        return_object["other"] = [{
            "table": "obs",
            "limiting_conditions": [{
                "table": "concept_name",
                "type": "=",
                "col_name": "name",
                "value": stringy("Type of violence, GBV"),
                "form_d": ""
            }],
            "values": [stringy("Physical, GBV"), stringy("Sexual, GBV"), stringy("Psychological"), stringy("Mixed, GBV")]
        }]

    return return_object


def find_labels(init_label,curr_sheet, curr_row):
    common = init_label.rstrip(" 1")
    count = 1
    temp_labels: list[str] = []
    while init_label is not None:
        init_label = curr_sheet[f'A{curr_row}'].value
        if init_label is not None and common + f' {count}' == curr_sheet[f'A{curr_row}'].value:
            temp_labels.append(stringy(init_label))
            count = count + 1
        else:
            return temp_labels
        curr_row = curr_row + 1
    if temp_labels.__len__() == 0:
        temp_labels.append(common)
    return temp_labels


def find_ranges(init_label,curr_sheet, curr_row, d_by_age, d_by_g, d_by_o):
    increament = 1
    if d_by_age and d_by_g:
        increament = 2
    if d_by_age and d_by_g and d_by_o:
        increament = 10
    common = init_label.rstrip(" 1")
    count = 1
    temp_values: list[int] = []
    temp_strings: list[str] = []
    first = True
    while init_label is not None:
        init_label = curr_sheet[f'A{curr_row}'].value
        if init_label is not None and common + f' {count}' == curr_sheet[f'A{curr_row}'].value:
            if d_by_age:
                x = curr_sheet[f'B{curr_row}'].value
                if d_by_o:
                    x = x.split(", ")[1]
            else:
                x = curr_sheet[f'C{curr_row}'].value
            if x:
                if re.search("^<=\W*[0-9.]+", x):
                    temp_values.append(-1000)
                    temp_values.append(int(re.findall("[0-9.]+", x)[0]))
                elif re.search("^<\W*[0-9.]+", x):
                    temp_values.append(-1000)
                    temp_values.append(int(re.findall("[0-9.]+", x)[0]) - 1)
                elif re.search("^[0-9.]+\W*-\W*[0-9.]+", x) or re.search("^[0-9.]+\W*to\W*[0-9.]+", x):
                    if first:
                        temp_values.append(int(re.findall("[0-9.]+", x)[0]) - 1)
                    temp_values.append(int(re.findall("[0-9.]+", x)[1]))
                elif re.search("<=\W*[0-9.]+", x):
                    temp_values.append(int(re.findall("[0-9.]+", x)[1]))
                elif re.search("^>\W*=\W*[0-9.]+", x):
                    temp_values.append(1000)
                elif re.search("^>\W*[0-9.]+", x):
                    temp_values.append(1000)
                else:
                    temp_strings.append(x)
            count = count + increament
        else:
            if len(temp_values) > 0:
                return temp_values
            if len(temp_strings) > 0:
                return temp_strings
        curr_row = curr_row + increament
        first = False
    if len(temp_values) > 0:
        return temp_values
    elif len(temp_strings) > 0:
        return temp_strings
    else:
        return temp_strings


def get_closest(curr_sheet, curr_row, curr_col):
    while curr_row > 0:
        if curr_sheet[f'{curr_col}{curr_row}'].value:
            return curr_sheet[f'{curr_col}{curr_row}'].value
        curr_row = curr_row -1
    return "Missing"

def get_Disaggregator(curr_sheet, curr_row):
    curr_col = "D"
    while curr_row > 0:
        if curr_sheet[f'{curr_col}{curr_row}'].value:
            return curr_sheet[f'C{curr_row}'].value
        curr_row = curr_row -1
    return "Missing"

def stringy(given_string):
    return '"' + given_string + '"'


def set_limitation_for_multi(raw_lim):
    limitation = {"missing": True}
    if not raw_lim:
        return limitation
    act_value = raw_lim.split('\"')
    parsed_condition = parse_condition(raw_lim)
    if 'under' in raw_lim and len(act_value) > 3:
        limitation = {
            "table": "obs",
            "type": "=",
            "col_name": "voided",
            "limiting_conditions": [
                {
                    "table": "concept_name",
                    "type": "=",
                    "force_key": "concept_id",
                    "col_name": "name",
                    "value": f"\"{act_value[3]}\""
                },
                {
                    "table": "concept_name",
                    "alias": "answer",
                    "force_key": "value_coded",
                    "type": "=",
                    "col_name": "name",
                    "value": f"\"{act_value[1]}\""
                }
            ],
            "value": "0"
        }
    elif len(act_value) > 1:
        act_value = raw_lim.split('\"')
        limitation = {
            "table": "obs",
            "type": "=",
            "col_name": "voided",
            "limiting_conditions": [
                {
                    "table": "concept_name",
                    "type": "=",
                    "force_key": "concept_id",
                    "col_name": "name",
                    "value": f"\"{act_value[1]}\""
                }
            ],
            "value": "0"
        }
    if parsed_condition:
        limitation["parsed_condition"]= parsed_condition
    return limitation


def set_limitation_for_single(curr_sheet, curr_row):
    limitation = {"missing": True}
    d_value = get_closest(curr_sheet, curr_row, "D")
    # if not curr_sheet[f'D{curr_row}'].value:
    #     return limitation
    act_value = d_value.split('\"')
    parsed_condition = parse_condition(d_value)
    if 'under' in d_value and len(act_value) > 3:
        limitation = {
            "table": "obs",
            "type": "=",
            "col_name": "voided",
            "limiting_conditions": [
                {
                    "table": "concept_name",
                    "type": "=",
                    "force_key": "concept_id",
                    "col_name": "name",
                    "value": f"\"{act_value[3]}\""
                },
                {
                    "table": "concept_name",
                    "alias": "answer",
                    "force_key": "value_coded",
                    "type": "=",
                    "col_name": "name",
                    "value": f"\"{act_value[1]}\""
                }
            ],
            "value": "0"
        }
    elif len(act_value) > 1:
        act_value = d_value.split('\"')
        limitation = {
            "table": "obs",
            "type": "=",
            "col_name": "voided",
            "limiting_conditions": [
                {
                    "table": "concept_name",
                    "type": "=",
                    "force_key": "concept_id",
                    "col_name": "name",
                    "value": f"\"{act_value[1]}\""
                }
            ],
            "value": "0"
        }
    if parsed_condition:
        limitation["parsed_condition"]= parsed_condition
    return limitation

excel_folder = "./excel_sheets"
# excel_folder = "./buta_csv"
filenames = next(walk(excel_folder), (None, None, []))[2]

reports = []
num_queries = 0
num_singles = 0
incomplete_queries = 21.5
for excel_file in filenames:
    just_name = excel_file.split(".")[0]
    full_path = f'{excel_folder}/{excel_file}'
    xlsx_file = Path(full_path)
    if xlsx_file.is_file() and full_path.endswith('.xlsx'):
        wb_obj = openpyxl.load_workbook(xlsx_file)
        sheet = wb_obj.active

        for row in range(1, 1000):
            if sheet[f"D{row}"].value:
                num_queries = num_queries + 1

            if sheet[f"A{row}"].value:
                label = sheet[f"A{row}"].value
                is_a_sub = row > 1 and sheet[f"A{row - 1}"].value and (sheet[f"A{row - 1}"].value + ". 1" == label or sheet[f"A{row - 1}"].value + ".1" == label)

                if isinstance(label, int):
                    print(label)
                    print(sheet)
                    print(row)
                if label.endswith('.'):
                    append_singular(sheet,row,label,just_name,reports, True)
                    num_singles = num_singles + 1

                elif row > 1 and is_a_sub and label.endswith('1'):
                    limitation_data = get_closest(sheet, row, "D")
                    aggregation_data = get_closest(sheet, row, "C")
                    aggr_by = ""
                    age_type = "YEAR"
                    if sheet[f"C{row - 1}"].value:
                        aggr_by = sheet[f"C{row - 1}"].value
                    elif sheet[f"C{row}"].value:
                        aggr_by = sheet[f"C{row}"].value
                    # else:

                        # print(just_name)
                        # print(sheet[f"A{row}"].value)

                    disaggregate_by_age = False
                    disaggregate_by_gender = False
                    disaggregate_by_other = False
                    if re.search("disaggregate.+?age", aggr_by.lower()):
                        disaggregate_by_age = True
                        if "month" in sheet[f"B{row}"].value:
                            age_type = "MONTH"
                    if re.search("disaggregate.+?sex", aggr_by.lower()):
                        disaggregate_by_gender = True
                    if label == "LG. 1" or label == "MS_ASTC.2. 1" or label == "MS_ASSTECH. 1":
                        disaggregate_by_other = True

                    lim = [set_limitation_for_multi(limitation_data)]
                    labels = find_labels(label, sheet, row)
                    values = find_ranges(label, sheet, row, disaggregate_by_age, disaggregate_by_gender, disaggregate_by_other)
                    catagorized_by = find_aggregation(values, aggregation_data, disaggregate_by_age, aggr_by , disaggregate_by_other)
                    reports.append({
                        "report_name": sheet[f"A{row}"].value.rstrip('.'),
                        "report_group": just_name,
                        "primary_counted_object": "person",
                        "labels": labels,
                        "categorized_by": catagorized_by,
                        "limiting_conditions": lim,
                        "age_type": age_type
                    })
                elif sheet[f"D{row}"].value and sheet[f"A{row + 1}"].value:
                    if sheet[f"A{row}"].value + ". 1" != sheet[f"A{row + 1}"].value:
                        append_singular(sheet, row, label, just_name, reports, False)
                        num_singles = num_singles + 1
                elif sheet[f"D{row}"].value:
                    append_singular(sheet, row, label, just_name, reports, False)

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(reports, f, ensure_ascii=False, indent=4)


# print(f'Parsed queries:{reports.__len__()}')
# print(f'Total number of queries:{num_queries}')
# print(f'Queries Completion:{reports.__len__()/num_queries * 100}%')
