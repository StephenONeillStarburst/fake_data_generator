from generate_data import GenerateData
import json
import connection

if __name__ == "__main__":

    with open('data_set.json') as json_file:
        data_set = json.load(json_file)
        
        connection_details = data_set['connection_details']

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
            column_string_types = generate_data.get_column_string_types()
            column_string = generate_data.get_column_string()

            #conn.create_postgres_table(connection_details['catalog'], connection_details['schema'], table_details['name'], column_string_types)
            conn.insert_value_arr_postgres_table(connection_details['catalog'], connection_details['schema'], table_details['name'], column_string, data)
    
    # #Create Postgres Tables
    

