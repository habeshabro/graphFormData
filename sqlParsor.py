import json
import os
import re

f = open('js.json')
f2 = open('data.json')
schema = json.load(f)
query_objects = json.load(f2)
num_incomplete = 0


def parsed_builder(query_object):
    if "parsed_condition" in query_object:
        return query_object["parsed_condition"]["more_joins"]
    elif 'limiting_conditions' in query_object:
        return parsed_builder(query_object["limiting_conditions"][0])
    else:
        return ""

def more_where(query_object):
    if "parsed_condition" in query_object:
        return query_object["parsed_condition"]["more_where"]
    elif 'limiting_conditions' in query_object:
        return more_where(query_object["limiting_conditions"][0])
    else:
        return ""


def condition_builder(lim_con):
    op1 = f"{lim_con['table']}.{lim_con['col_name']}"
    lim_type = lim_con['type']
    op2 = lim_con['value']
    if lim_type in ["=", ">", ">=", "<", "<=", "!=", "LIKE", "IS NOT", "IS"]:
        return f"{op1} {lim_type} {op2}"
    return "Missing builder"


def find_link_condition(tb1, tb2):
    if [x for x in schema[tb1]["fk"] if x["refrences"].split('.')[0] == tb2]:
        conditions = [x for x in schema[tb1]["fk"] if x["refrences"].split('.')[0] == tb2][0]
        return f"{tb1}.{conditions['field']} = {conditions['refrences']}"
    if [x for x in schema[tb2]["fk"] if x["refrences"].split('.')[0] == tb1]:
        conditions = [x for x in schema[tb2]["fk"] if x["refrences"].split('.')[0] == tb1][0]
        return f"{tb2}.{conditions['field']} = {conditions['refrences']}"
    return "MISSING LINK"


def find_sub_conditions(tb1, lim_con):
    if 'missing' in lim_con:
        return "MISSING CONDITION"
    tb2 = lim_con['table']
    oth_cond = condition_builder(lim_con)
    collector = ""
    if "force_key" in lim_con:
        if [x for x in schema[tb1]["fk"] if x["refrences"].split('.')[0] == tb2 and x["field"] == lim_con["force_key"]]:
            conditions = [x for x in schema[tb1]["fk"] if x["refrences"].split('.')[0] == tb2 and x["field"] == lim_con["force_key"]][0]
            if 'limiting_conditions' in lim_con:
                for cond in lim_con['limiting_conditions']:
                    collector = collector + "AND " + find_sub_conditions(tb2, cond) + "\n"
                return f"{tb1}.{conditions['field']} in (SELECT {tb1}.{conditions['field']} from {tb1} JOIN {tb2} ON {tb1}.{conditions['field']} = {conditions['refrences']} AND {oth_cond} {collector})"
            return f"{tb1}.{conditions['field']} in (SELECT {tb1}.{conditions['field']} from {tb1} JOIN {tb2} ON {tb1}.{conditions['field']} = {conditions['refrences']} AND {oth_cond})"
    if [x for x in schema[tb1]["fk"] if x["refrences"].split('.')[0] == tb2]:
        conditions = [x for x in schema[tb1]["fk"] if x["refrences"].split('.')[0] == tb2][0]
        if 'limiting_conditions' in lim_con:
            for cond in lim_con['limiting_conditions']:
                collector = collector + "AND " + find_sub_conditions(tb2, cond) + "\n"
            return f"{tb1}.{conditions['field']} in (SELECT {tb1}.{conditions['field']} from {tb1} JOIN {tb2} ON {tb1}.{conditions['field']} = {conditions['refrences']} AND {oth_cond} {collector})"
        return f"{tb1}.{conditions['field']} in (SELECT {tb1}.{conditions['field']} from {tb1} JOIN {tb2} ON {tb1}.{conditions['field']} = {conditions['refrences']} AND {oth_cond})"
    if [x for x in schema[tb2]["fk"] if x["refrences"].split('.')[0] == tb1]:
        conditions = [x for x in schema[tb2]["fk"] if x["refrences"].split('.')[0] == tb1][0]
        if 'limiting_conditions' in lim_con:
            for cond in lim_con['limiting_conditions']:
                collector = collector + "AND " + find_sub_conditions(tb2, cond) + "\n"
            return f"{conditions['refrences']} in (SELECT {conditions['refrences']} from {tb1} JOIN {tb2} ON {tb2}.{conditions['field']} = {conditions['refrences']} AND {oth_cond} {collector}) "
        return f"{conditions['refrences']} in (SELECT {conditions['refrences']} from {tb1} JOIN {tb2} ON {tb2}.{conditions['field']} = {conditions['refrences']} AND {oth_cond})"
    return "MISSING LINK"


def query_creator(query_object):
    path = "generated/"+query_object["report_group"]
    file_name = f'{path}/{query_object["report_name"]}.sql'
    is_exist = os.path.exists(path)
    if not is_exist:
        os.makedirs(path)

    query_string = "SELECT \n"
    cases = []

    if query_object['categorized_by']['other']:
        for oth_case in query_object['categorized_by']['other']:
            if oth_case['values'].__len__() > 0 and isinstance(oth_case['values'][0], int):
                time_case = []
                for idVal, val in enumerate(oth_case['values']):
                    if idVal < len(oth_case['values']) - 1:
                        if oth_case['values'][idVal] == -1000:
                            time_case.append(f"obs.value_numeric <= {oth_case['values'][idVal + 1]}")
                        elif oth_case['values'][idVal + 1] == 1000:
                            time_case.append(f"obs.value_numeric > {oth_case['values'][idVal]}")
                        else:
                            time_case.append(
                                f"obs.value_numeric between {oth_case['values'][idVal] + 1} and {oth_case['values'][idVal + 1]}")
                cases.append(time_case)
            elif oth_case['values'].__len__() > 0 and isinstance(oth_case['values'][0], str):
                time_case = []
                for idVal, val in enumerate(oth_case['values']):
                    if re.search("other than \".+?\"", val):
                        val = re.findall("\".+?\"", val)[0]
                        time_case.append(f"answer.name NOT LIKE {val}")
                    elif re.search("\".+?\" or \".+?\"", val, re.IGNORECASE):
                        vals = re.findall("\".+?\"", val)
                        to_append = ""
                        first_val = True
                        for x in vals:
                            if first_val:
                                to_append = to_append + f"answer.name LIKE {x}"
                                first_val = False
                            else:
                                to_append = to_append + f" OR answer.name LIKE {x}"
                        time_case.append(to_append)
                    else:
                        val = re.findall("\".+?\"", val)[0]
                        time_case.append(f"answer.name LIKE {val}")
                cases.append(time_case)

    if query_object['categorized_by']['age']:
        age_case = []
        for idAge, age in enumerate(query_object['categorized_by']['age']):
            if idAge < len(query_object['categorized_by']['age']) - 1:
                if query_object['categorized_by']['age'][idAge] == -1000:
                    age_case.append(
                        f"TIMESTAMPDIFF({query_object['age_type']}, person.birthdate, v.date_started) <= {query_object['categorized_by']['age'][idAge + 1]}")
                elif query_object['categorized_by']['age'][idAge + 1] == 1000:
                    age_case.append(
                        f"TIMESTAMPDIFF({query_object['age_type']}, person.birthdate, v.date_started) > {query_object['categorized_by']['age'][idAge]}")
                elif isinstance(query_object['categorized_by']['age'][idAge], int):
                    age_case.append(
                        f"TIMESTAMPDIFF({query_object['age_type']}, person.birthdate, v.date_started) between {query_object['categorized_by']['age'][idAge] + 1} and {query_object['categorized_by']['age'][idAge + 1]}")
                else:
                    print(query_object)
        cases.append(age_case)

    if query_object['categorized_by']['gender']:
        cases.append(["person.gender = 'M'", "person.gender = 'F'"])

    p_obj = query_object["primary_counted_object"]
    labels = query_object["labels"]
    p_obj_key = schema[p_obj]["pk"][0]
    p_obj_fks = schema[p_obj]["fk"]
    if not cases:
        if labels.__len__() == 0:
            query_string = query_string + f'COUNT(DISTINCT {p_obj}.{p_obj_key}) AS \"{query_object["report_name"]}\",\n'
        else:
            query_string = query_string + f'COUNT(DISTINCT {p_obj}.{p_obj_key}) AS {labels[0]},\n'
    else:
        counter = 0
        if len(cases) == 1:
            for label_id, case in enumerate(cases[0]):
                if labels.__len__() > label_id:
                    query_string = query_string + f'COUNT(DISTINCT CASE WHEN {case} THEN {p_obj}.{p_obj_key} END) AS {labels[counter]},\n'
                    counter = counter + 1
                else:
                    print(labels)
                    query_string = query_string + f'COUNT(DISTINCT CASE WHEN {case} THEN {p_obj}.{p_obj_key} END) AS LABEL LIST TOO SHORT,\n'
        elif len(cases) == 2:
            for label_id1, case in enumerate(cases[0]):
                for label_id2, case2 in enumerate(cases[1]):
                    query_string = query_string + f'COUNT(DISTINCT CASE WHEN {case} AND {case2} THEN {p_obj}.{p_obj_key} END) AS {labels[counter]},\n'
                    counter = counter + 1
        elif len(cases) == 3:
            for label_id1, case in enumerate(cases[0]):
                for label_id2, case2 in enumerate(cases[1]):
                    for label_id3, case3 in enumerate(cases[2]):
                        query_string = query_string + f'COUNT(DISTINCT CASE WHEN {case} AND {case2} AND {case3} THEN {p_obj}.{p_obj_key} END) AS {labels[counter]},\n'
                        counter = counter + 1
    remove_trailing = query_string.rstrip("\n,")
    limiting_condition_cat = ""
    limiting_condition_cat2 = "fjksdbfjkdsfbsk"
    is_limiting = False
    query_string = remove_trailing + f'\nFROM {p_obj}\n'
    # query_string = query_string + "JOIN encounter on person.person_id = encounter.patient_id AND date(encounter.encounter_datetime) between '#startDate#' and '#endDate#' \n"
    query_string = query_string + "JOIN visit v ON person.person_id = v.patient_id AND date(v.date_started) between '#startDate#' and '#endDate#' \n"
    if query_object['categorized_by']['other']:
        for join_object in query_object['categorized_by']['other']:
            table_name = join_object['table']
            condition = find_link_condition(table_name, p_obj)
            if join_object['limiting_conditions']:
                for limiting_condition in join_object['limiting_conditions']:
                    if "col_name" in limiting_condition and limiting_condition["col_name"] == "name":
                        is_limiting = True
                        limiting_condition_cat = limiting_condition["value"]
                        if limiting_condition["form_d"]:
                            limiting_condition_cat2 = limiting_condition["form_d"]



                    # othCondition = condition_builder(limiting_condition)
                    # sub_conditioner = find_sub_conditions(table_name, limiting_condition)
                    condition = condition

            query_string = query_string + f"JOIN {table_name} ON {condition}\n"

    if "answer.name" in query_string:
        query_string = query_string + "JOIN concept_name answer ON answer.concept_id = obs.value_coded AND answer.concept_name_type = 'FULLY_SPECIFIED'\n"

    query_string = query_string + parsed_builder(query_object) + "\n"
    query_string = query_string.replace("**","%%")

    if "single_dissagregator" in query_object and query_object["single_dissagregator"] != "":
        query_string = query_string + f"JOIN obs ON obs.encounter_id = encounter.encounter_id\n"
        query_string = query_string + f"JOIN concept_name primo_name ON primo_name.concept_id = obs.concept_id AND primo_name.concept_name_type = 'FULLY_SPECIFIED' AND primo_name.name LIKE {query_object['sd_by']}\n"
        query_string = query_string + f"JOIN concept_name answer ON answer.concept_id = obs.value_coded AND answer.concept_name_type = 'FULLY_SPECIFIED' AND answer.name LIKE {query_object['single_dissagregator']}\n"

    if is_limiting:
        if limiting_condition_cat in query_string:
            first = True
            limiting_condition_cat = limiting_condition_cat.replace("(", "\\(")
            limiting_condition_cat = limiting_condition_cat.replace(")", "\\)")
            wh_cond = f"(?<=AND )[^\s]+?(?=.concept_id IN .+? {limiting_condition_cat})"
            where_conditions = re.findall(wh_cond, query_string)
            # where_conditions = re.findall("JOIN obs .+? ON", query_string)
            # print(where_conditions)
            for where_condition in where_conditions:
                # where_condition = where_condition.replace("JOIN obs ", "").replace(" ON", "")
                if first:
                    where_condition = f"\nWHERE obs.obs_id = {where_condition}.obs_id"
                    first = False
                else:
                    where_condition = f"\n OR obs.obs_id = {where_condition}.obs_id"
                query_string = query_string + where_condition
            if first:
                print(limiting_condition_cat)
                print(query_string)
                print(where_conditions)

        elif limiting_condition_cat2 in query_string:
            first = True
            where_conditions = re.findall("JOIN obs .+? ON", query_string)
            for where_condition in where_conditions:
                where_condition = where_condition.replace("JOIN obs ", "").replace(" ON", "")
                if first:
                    where_condition = f"\nWHERE obs.obs_group_id = {where_condition}.obs_id"
                    first = False
                else:
                    where_condition = f"\n OR obs.obs_group_id = {where_condition}.obs_id"
                where_condition = where_condition + f' AND obs.concept_id IN (SELECT concept_id from concept_name WHERE concept_name_type = "FULLY_SPECIFIED" AND name LIKE {limiting_condition_cat})'
                query_string = query_string + where_condition

        else:
            query_string = query_string + f"JOIN concept_name primo_name ON primo_name.concept_id = obs.concept_id AND primo_name.concept_name_type = 'FULLY_SPECIFIED' AND primo_name.name LIKE {limiting_condition_cat}\n"



    if is_limiting:
        query_string = query_string + f" AND ({more_where(query_object)})\n"
    else:
        query_string = query_string + f"WHERE ({more_where(query_object)})\n"

    query_string = query_string.replace('LIKE "!!', 'NOT LIKE "')
    query_string = query_string.replace('"Yes"', '"True"')
    # if query_object['limiting_conditions']:
    #     prep = "WHERE"
    #     for lim_con in query_object['limiting_conditions']:
    #         # othCondition = condition_builder(lim_con)
    #         sub_conditioner = find_sub_conditions(p_obj, lim_con)
    #         query_string = query_string + f"{prep} {sub_conditioner}"
    #         prep = "AND"

    with open(file_name, 'w', encoding='utf-8') as f3:
        f3.write(query_string)
    return query_string


for qo in query_objects:
    query_creator(qo)


f.close()
f2.close()
