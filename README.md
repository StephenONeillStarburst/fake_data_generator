# fake_data_generator


### Helpful Tips when Running the Script

1. Here is an example data_set.json file that you would need to populate 

{
    "connection_details": {
        ### local = local dir file, s3 = parquet file in s3, sql = sql table
        "output": "<local/s3/sql>",
        "host": "<host>",
        "port": 443,
        "user": "<username>",
        "password": "<password>",
        "catalog": "",
        "schema": ""
    },
    "tables": [
        {
            "table": {
                "name": "test_table",
                "row_count": 50,
                "columns": [
                    {"column_name": "<field_1>", "column_data_type": "<date/varchar/etc..>", "column_primary_key": <true/false>, "column_data_populate_type": "<date_since/random_string/etc..>"}
                ]
                
            }
        }
    ] 
}