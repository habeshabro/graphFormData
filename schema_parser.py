import re
import json

def main():
    with open('schema.sql', 'r') as reader:
        schema = {}
        current_table_name = "start"
        currently_writing = False
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            if "CREATE TABLE" in line:
                currently_writing = True
                current_table_name = re.search('`(.+?)`', line).group(1)
                schema[current_table_name] = {'data': [], 'keys': [], 'pk': [], 'fk': []}
            elif "PRIMARY KEY" in line:
                schema[current_table_name]['pk'] = re.search('\((.+?)\)', line).group(1).replace('`','').split(',')
                currently_writing = False
            elif "FOREIGN KEY" in line:
                schema[current_table_name]['fk'].append(
                    {
                        "field": re.findall('`(.+?)`', line)[1],
                        "refrences": re.findall('`(.+?)`', line)[2] + '.' + re.findall('`(.+?)`', line)[3]
                    }
                )
                currently_writing = False
            elif "KEY " in line:
                schema[current_table_name]['keys'].append(line.strip())
                currently_writing = False
            elif ") ENGINE=InnoDB" in line:
                currently_writing = False
            elif currently_writing:
                schema[current_table_name]['data'].append(re.search('`(.+?)`', line).group(1))
            line = reader.readline()
    with open('js.json', 'w') as writer:
        json_object = json.dumps(schema, indent=4)
        writer.write(json_object)


if __name__ == "__main__":
    main()
