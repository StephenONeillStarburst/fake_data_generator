from random import *

import string
from faker import Faker

# Arr with all fifty states
states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

class GenerateData:
    def __init__(self, table_columns, table_details) -> None:
        self.table_columns = table_columns
        self.table_details = table_details

    def generate(self):
        arr = []

        print("Starting to generate data... This may take a while.")
            
        table_data = self.create_table_data(self.table_columns, self.table_details['row_count'])
        return table_data

    # JSON representation of the data set
    def create_table_data(self, table_columns, row_count):
        table_data = []
        for i in range(0, row_count):
            dict_row = {}
            for column in table_columns:
                dict_row[column['column_name']] = self.create_column_data(column, i)
            table_data.append(dict_row)

        return table_data
    
    
    def create_column_data(self, column, primary_count):
        if column.get('column_data_array'):
            return choice(column.get('column_data_array'))
        
        if column.get('column_primary_key'):
            if column.get('column_data_populate_type') == 'number':
                return primary_count
            
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

