from random import *

import string
from faker import Faker
import threading
from tqdm import tqdm

import csv
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pyarrow.fs
import logging


# Arr with all fifty states
states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

class GenerateData:
    def __init__(self, table_columns, table_details) -> None:
        self.table_columns = table_columns
        self.table_details = table_details
        self.progress_bar = None
        
    def update_progress_bar(self):
        self.progress_bar.update(1)
            
    def close_progress_bar(self):
        self.progress_bar.close()

    def generate(self):
        print("Starting to generate data... This may take a while.")

        threads = []
        table_data = []
        
        # Initialize tqdm progress bar
        self.progress_bar = tqdm(total=self.table_details['row_count'], desc="Generating data")

        for _ in range(self.table_details['row_count']):
            t = threading.Thread(target=self.create_row, args=(table_data,))
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()
        
            
        # Close tqdm progress bar
        self.close_progress_bar()

        return table_data
    
    # Function to create a single row of data
    def create_row(self, table_data):
        dict_row = {}
        for column in self.table_columns:
            dict_row[column['column_name']] = self.create_column_data(column)
        table_data.append(dict_row)
        self.update_progress_bar()  # Update progress bar


    # JSON representation of the data set
    def create_table_data(self, table_columns, row_count):
        table_data = []
        for i in range(0, row_count):
            dict_row = {}
            for column in table_columns:
                dict_row[column['column_name']] = self.create_column_data(column, i)
            table_data.append(dict_row)

        return table_data
    
    
    # Function to create data for a single column
    def create_column_data(self, column):
        if column.get('column_data_array'):
            return choice(column.get('column_data_array'))

        if column.get('column_primary_key'):
            if column.get('column_data_populate_type') == 'number':
                return threading.get_ident()  # Thread-specific primary key

        if column.get('column_data_populate_type') == 'random_string':
            string_length = column.get('column_string_length') if column.get('column_string_length') else 10
            return ''.join(choices(string.ascii_uppercase +
                                   string.digits, k=string_length))
        
        if column.get('column_default_value'):
            return column.get('column_default_value')
            
        if column.get('column_data_populate_type') == 'number':
            min = column.get('column_data_min') if column.get('column_data_min') is not None else None
            max = column.get('column_data_max') if column.get('column_data_max') is not None else None

            if min is not None and max is not None:
                return randint(min, max)
            return randint(1, 1000000)
        
        if column.get('column_data_populate_type') == 'first_name':
            return Faker().first_name()
        
        if column.get('column_data_populate_type') == 'last_name':
            return Faker().last_name()
        
        if column.get('column_data_populate_type') == 'full_name':
            return Faker().name()
        
        if column.get('column_data_populate_type') == 'country':
            return Faker().country()
        
        if column.get('column_data_populate_type') == 'state':
            return choice(states)

        if column.get('column_data_populate_type') == 'address':
            return Faker().address()
        
        if column.get('column_data_populate_type') == 'phone_number':
            return Faker().phone_number()
        
        if column.get('column_data_populate_type') == 'date_since':
            return f"{randint(2020, 2023)}-{randint(1, 12):02d}-{randint(1, 28):02d}"
        
        if column.get('column_data_populate_type') == 'company_name':
            return Faker().company()
        
        if column.get('column_data_populate_type') == 'color':
            return Faker().color()
        
        if column.get('column_data_populate_type') == 'random_bigint':
            return Faker().random_int()
        
        if column.get('column_data_populate_type') == 'job':
            return Faker().job()
        
        if column.get('column_data_populate_type') == 'word':
            return Faker().word()


    def get_column_string_types(self):
        column_string = "("
        for column in self.table_columns:
            column_string += column['column_name'] + " " + column['column_data_type'] + ", "
        
        column_string = column_string[:-2]
        column_string += ")"
        return column_string
    
    def get_column_string(self):
        column_string = "("
        for column in self.table_columns:
            column_string += column['column_name'] + ","
        
        column_string = column_string[:-1]
        column_string += ")"
        return column_string
    
    def write_to_local_csv(self, data):
        with open(self.table_details['name'] + '.csv', 'w', newline='') as csvfile:
            keys = data[0].keys()
            writer = csv.DictWriter(csvfile, keys)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    def write_parquet_to_s3_in_chunks(self, df, s3_bucket, s3_key, chunk_size=10000000):
        # Create an S3FileSystem object
        s3 = pyarrow.fs.S3FileSystem()

        table_name = self.table_details["name"]
        # Specify the S3 URI
        s3_uri = f"{s3_bucket}/{s3_key}/{table_name}"
        logging.info(s3_uri)

        # Determine the total number of chunks
        num_chunks = (len(df) + chunk_size - 1) // chunk_size

        # Write each chunk to S3
        for i in range(num_chunks):
            # Get the start and end indices for the current chunk
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))

            # Get the current chunk of data
            chunk = df[start_idx:end_idx]
            
            try:
                # Convert the chunk to a Pandas DataFrame
                chunk_df = pd.DataFrame(chunk)

                # Convert the chunk DataFrame to a PyArrow Table
                table = pa.Table.from_pandas(chunk_df)

                # Convert the PyArrow Table to Parquet format in memory
                buffer = pa.BufferOutputStream()
                pq.write_table(table, buffer)

                # Construct the S3 key for the current chunk
                chunk_key = f"{s3_uri}/chunk_{i}.parquet"

                # Write the Parquet data from memory to the specified S3 location
                with s3.open_output_stream(chunk_key) as f:
                    f.write(buffer.getvalue())
            except Exception as e:
                logging.error(f"Error writing chunk {i} to S3: {e}")
                logging.error(f"Format of chunk key {chunk_key}")

