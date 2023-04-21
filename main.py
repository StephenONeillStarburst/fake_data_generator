from generate_data import GenerateData
import json
import connection
import csv

if __name__ == "__main__":

    with open('data_set.json') as json_file:
        is_csv = False
        conn = None

        data_set = json.load(json_file)

        connection_details = data_set['connection_details']

        if connection_details['type'] == 'csv':
            is_csv = True
        else:
            conn = connection.Connection(        
                trino_host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details['password'],
                catalog=connection_details['catalog'],
                schema=connection_details['schema']
            )
        
        for table in data_set['tables']:
            
            table_details = table['table']
            table_columns = table_details['columns']

            generate_data = GenerateData(table_columns=table_columns, table_details=table_details)
            data = generate_data.generate()
            
            if len(data) == 0:
                print("No data to generate")
                continue

            column_string_types = generate_data.get_column_string_types()
            column_string = generate_data.get_column_string()

            if is_csv:
                #1) Open a csv file and write the data to it
                with open(table_details['name'] + '.csv', 'w', newline='') as csvfile:
                    keys = data[0].keys()
                    writer = csv.DictWriter(csvfile, keys)
                    writer.writeheader()
                    for row in data:
                        writer.writerow(row)
                csvfile.close()
            else:
                conn.insert_value_arr_table(connection_details['catalog'], connection_details['schema'], table_details['name'], column_string, data)
    
    #Create Postgres Tables
    

