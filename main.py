from generate_data import GenerateData
import json
import connection

if __name__ == "__main__":

    with open('data_set.json') as json_file:
        conn = None

        data_set = json.load(json_file)

        connection_details = data_set['connection_details']
        output_details = data_set['output_details']

        if connection_details['output'] == 'sql':
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

            if connection_details['output'] == 'local':
                #1) Open a csv file and write the data to it
                generate_data.write_to_local_csv(data)
            elif connection_details['output'] == 's3':
                # Write the parquet data to s3
                generate_data.write_parquet_to_s3_in_chunks(data, output_details['s3_bucket'], output_details['bucket_path'])
            else:
                conn.insert_value_arr_table(connection_details['catalog'], connection_details['schema'], table_details['name'], column_string, data)
    
    #Create Postgres Tables
    

