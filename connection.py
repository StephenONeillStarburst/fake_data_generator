import trino

class Connection:
    def __init__(self, trino_host, port, user, password, catalog, schema) -> None:

        self.conn = trino.dbapi.connect(
            host=trino_host,            # Required. update with hostname
            port=port,                  # Required. update with trino port
            user=user,                  # Required. update with user to connect to trino with
            catalog=catalog,            # Optional. update with catalog
            schema=schema,              # Optional. update with schema
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(user, password),
            roles={catalog: "sysadmin"},
        )   
        
    # execute query
    def run_query(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return(rows)
    

    def create_s3_schema(self, schema, s3_location):
        query = f"create schema if not exists {schema} with (location='s3a://{s3_location}')"
        self.run_query(query)
    
    # CAUTION: This will drop the table if it exists.
    # columns parameter should start and end with ( and )
    def create_table(self, catalog, schema, table, columns):
        schema_query = f'create schema if not exists {catalog}.{schema}'
        self.run_query(schema_query)

        drop_query = f'drop table if exists {catalog}.{schema}.{table}'
        self.run_query(drop_query)

        create_query = f"create table {catalog}.{schema}.{table} {columns}"
        self.run_query(create_query)
    
    # Requires a single value.
    def insert_value_table(self, catalog, schema, table, columns, value):
        insert_query = f"insert into {catalog}.{schema}.{table} ({columns}) values ({value})"
        self.run_query(insert_query)
    
    # Requires a list of dictionaries.
    def insert_value_arr_table(self, catalog, schema, table, columns, values):
        insert_query = f"insert into {catalog}.{schema}.{table} {columns} values"
        values_query = ""
        row_count = 0
        total_count = 0
        
        # Iterate through each row
        for dict in values:
            string=""
            for key, value in dict.items():
                # If value is a string, add quotes
                if type(value) is str:
                    string += f"'{value}',"
                else:
                    string += f"{value},"
            
            # Remove last comma
            values_query += f"({ string[:-1] }),"
            row_count += 1
            
            # Insert every 20,000 rows
            # 
            if(row_count == 20000):
                total_count += row_count
                self.run_query(insert_query + " " + values_query[:-1])
                row_count = 0
                values_query = ""
                print(f"{total_count} rows inserted.")
        
        if(row_count > 0):
            total_count += row_count
            self.run_query(insert_query + " " + values_query[:-1])
            print(f"{total_count} rows inserted.")
    
    # Assumes that schema / table will be same in postgres and s3
    def copy_postgres_to_s3(self, postgres_catalog, s3_catalog, schema, table):
        query = f"create table {s3_catalog}.{schema}.{table} as select * from {postgres_catalog}.{schema}.{table}"
        self.run_query(query)
