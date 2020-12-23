#!python3
#pgsql.py - basic pgsql client for gulliver

import psycopg2
import json
import torontoopendata
import logging

class PGSQLClient:
    def __init__(self):
        # Init logging
        logging.basicConfig(format='%(levelname)s-%(asctime)s: %(message)s', level=logging.INFO, filename="runner.log")

        # fetch psql password
        self.password = open("./psql_password.txt", "r").read()
        

        self.type_map = {
                "int": "INT",
                "text": "TEXT",
                "timestamp": "TIMESTAMP",
                "float": "FLOAT(8)",
                "numeric": "NUMERIC",
                "date": "DATE",
                "time": "TIME"
            }  



    def connect(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="gulliver",
            user="postgres",
            password=self.password
        )

        return self.conn.cursor()

    def remove_numeric(self, input):
        return ''.join(i for i in input if not i.isdigit())

    def create_table(self, input):
        # check that the input has the fields, records and package keys
        if 'package' in input.keys() and 'fields' in input.keys() and 'records' in input.keys() and len(input["fields"]) > 0:

            

            query_string = "CREATE TABLE IF NOT EXISTS " + self.remove_numeric(input['package']).replace("-", "") + " ("
            for field in input['fields']:
                type = self.type_map[ self.remove_numeric(field["type"]) ]
                # write each row of the create table query
                if field["id"] == "_id":
                    query_string += field["id"] + " " + type + " PRIMARY KEY, "
                else:
                    query_string += field["id"].replace("?", "qq").replace("-", "").replace(" ", "").replace("#", "num").replace(".", "_") + " " + type + ", "
            query_string = query_string[:-2] + ")"

            self.run_query(query_string)

    
    def trunc_and_load_table(self, input):
        if 'package' in input.keys() and 'fields' in input.keys() and 'records' in input.keys() and len(input["fields"]) > 0:
            table_name = self.remove_numeric(input['package']).replace("-", "")
            truncate_query_string = "TRUNCATE TABLE " + table_name
            self.run_query( truncate_query_string )

            insert_query_string_base = "INSERT INTO " + table_name + " (" + ", ".join(field["id"].replace("?", "qq").replace("-", "").replace(" ", "").replace("#", "num").replace(".", "_") for field in input["fields"]) + ") "
            
            # in chunks of 100 records ...
            chunk_size = 100
            for chunk in range(0, len(input['records']), chunk_size):
                
                # init the beginning of an insert query with the table and field names ...
                insert_query_string = insert_query_string_base + " VALUES "
                # for each 100 records
                for i in range(0 + chunk, chunk + chunk_size-1, 1):
                    # for each record in the 100
                    if i < len(input["records"]):
                        insert_query_string += "(" 
                        # for each field in each record
                        for field in input["fields"]:
                            if self.type_map[ self.remove_numeric(field["type"]) ] == "INT":
                                if input["records"][i][field["id"]] in ("None", None, "NaN"):
                                    insert_query_string += "Null, "
                                else:
                                    insert_query_string +=  str(input["records"][i][field["id"]]) + ", "
                            
                            if self.type_map[ self.remove_numeric(field["type"]) ] == "TEXT":
                                insert_query_string += "'" + str(input["records"][i][field["id"]]).replace("'", "''").replace("\"", "\"\"") + "', "

                            if self.type_map[ self.remove_numeric(field["type"]) ] in ["TIMESTAMP", "DATE", "TIME"]:
                                if input["records"][i][field["id"]] in ("None", None):
                                    insert_query_string += "Null, "
                                else:
                                    insert_query_string += "'" + str(input["records"][i][field["id"]]) + "', "

                            if self.type_map[ self.remove_numeric(field["type"]) ] in ["FLOAT(8)", "NUMERIC"]:
                                if input["records"][i][field["id"]] in ("None", None, "NaN"):
                                    insert_query_string += "Null, "
                                else:
                                    insert_query_string += str(input["records"][i][field["id"]]) + ", "
                            
                            #insert_query_string += ", ".join(["'" + str(input["records"][i][field["id"]]).replace("'", "''") + "'" if field["type"].upper() in ("TEXT", "TIMESTAMP") else str(input["records"][i][field["id"].replace("?", "qq").replace("-", "")]).replace("None", "NULL") for field in input["fields"] ]) 
                        
                        insert_query_string = insert_query_string[:-2]
                        insert_query_string += "), "

                insert_query_string = insert_query_string[:-2]
                self.run_query( insert_query_string )
                
                

            

    def run_query(self, input):
        self.cursor = self.connect()
        self.cursor.execute(input)
        return self.conn.commit()


    
    
            



    
if __name__ == '__main__':

    todclient = torontoopendata.TorontoOpenDataClient()
    psqlclient = PGSQLClient()

    for package in todclient.package_list:
        if package not in  ["field-information-reports", "highrise-residential-fire-inspection-results"]: #field info reports too big, highrise fire has duplicate 'latitude' column
            print(package)
            try:
                package = todclient.return_package_data(package)
                
                psqlclient.create_table(package)
                psqlclient.trunc_and_load_table(package)

            except Exception as e:
                print("=========FAIL=============")
                print(e)