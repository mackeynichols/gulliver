
#!python3
#pgsql.py - basic pgsql client for gulliver

import psycopg2
import json
import torontoopendata
import logging
from scipy import stats
import matplotlib.pyplot as plt
import numpy as np
import math
import json



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
        logging.info("PGSQL Connected successfully")
        return self.conn.cursor()

    def remove_numeric(self, input):
        return ''.join(i for i in input if not i.isdigit())

    def create_table(self, input):
        # check that the input has the fields, records and package keys
        if 'package' in input.keys() and 'fields' in input.keys() and 'records' in input.keys() and len(input["fields"]) > 0:

            try:
                try:
                    self.run_query("DROP TABLE " + self.remove_numeric(input['package']).replace("-", ""))
                except Exception as e:
                    logging.error(e)
                query_string = "CREATE TABLE IF NOT EXISTS " + self.remove_numeric(input['package']).replace("-", "") + " ("
                for field in input['fields']:
                    type = self.type_map[ self.remove_numeric(field["type"]) ]
                    # write each row of the create table query
                    if field["id"] == "_id":
                        query_string += field["id"] + " " + type + " PRIMARY KEY, "
                    elif field["id"] == "geometry":
                        query_string += "geometry GEOMETRY, "
                    elif field["id"] not in ["_id", "geometry"]:
                        query_string += field["id"].replace("?", "qq").replace("-", "").replace(" ", "").replace("#", "num").replace(".", "_") + " " + type + ", "
                query_string = query_string[:-2] + ")"

                logging.info(query_string)
                self.run_query(query_string)
                logging.info("CREATE TABLE query run for " + self.remove_numeric(input['package']).replace("-", ""))
            except Exception as e:
                logging.error(e)
    
    def trunc_and_load_table(self, input):
        if 'package' in input.keys() and 'fields' in input.keys() and 'records' in input.keys() and len(input["fields"]) > 0:
            table_name = self.remove_numeric(input['package']).replace("-", "")
            truncate_query_string = "TRUNCATE TABLE " + table_name
            self.run_query( truncate_query_string )
            logging.info("TRUNCATE finished on " + input['package'].replace("-", ""))

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
                                # geometry fields are technically "text", so if a field is here and its called "geometry" then we need to parse out coords
                                # this currently only covers point data; polygons and lines will error out, and their tables wont be loaded
                                if field["id"] == "geometry":
                                    latlong = json.loads( input["records"][i][field["id"]] )["coordinates"]
                                    insert_query_string += "ST_GeomFromText('POINT" + str(latlong).replace("[", "(").replace("]", ")").replace(",", "") + "', 4326), "
                                else:
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



    def return_columns(self):
        # get a list of how many times a columm shows up, and and which tables it shows up for text/varchar columns not in system tables
        columns = {}
        for row in self.return_query_results("SELECT table_name, column_name, data_type, table_schema FROM information_schema.columns"):
            if row[1] in columns.keys() and row[3] not in ["information_schema", "pg_catalog"] and row[2] in ["text"]:
                columns[ row[1] ]["count"] += 1
                columns[ row[1] ]["tables"].append(row[0])
                columns[ row[1] ]["type"].append(row[2])
                columns[ row[1] ]["schema"].append(row[3])

            elif row[3] not in ["information_schema", "pg_catalog"] and row[2] in ["text"] and row[1] != "geometry":
                columns[ row[1] ] = {"count": 1, "tables": [ row[0] ], "type": [ row[2]], "schema": [ row[3]]  } 

        return columns

    def return_join_keys(self):
        # with a given list of "priority" columns (ie columns that appear many times, that could be used as join keys)
        # ... identify which tables share a high % of values in those join columns

        # output will be {column: {"table1+table2": percent_overlap}}
        output = {}

        # thresholds
        min_column_frequency = 2
        min_shared_records_percent = 0.50
        min_table_size = 100

        cols = self.return_columns()
        for col in cols.keys():
            if cols[col]["count"] >= min_column_frequency:
                print(col)
                output[col] = {}
                # check percent overlap between EACH table that has this column, and put that into an array
                for i in range( len(cols[col]["tables"]) - 1 ):
                    table1 = cols[col]["tables"][i]
                    for j in range(i, len(cols[col]["tables"]) - 1 ):
                        if i != j:
                            table2 = cols[col]["tables"][j]
                            query = "SELECT (SELECT COUNT(*) FROM "+table1+"), (SELECT COUNT(*) FROM "+table2+"),COUNT(*) FROM " + table1 + " i INNER JOIN " + table2 + " j ON i."+ col +" = j." + col
                            
                            result = self.return_query_results( query )[0]
                            if result[2] > 0 and result[1] > min_table_size and result[0] > min_table_size:
                                if result[0]/result[2] > min_shared_records_percent and result[1]/result[2] > min_shared_records_percent:
                                    print(col + ": " + table1 + " + " + table2)
                                    print([ result[0]/result[2], result[1]/result[2] ])

                                    output[col][table1 + " + " + table2] = [ result[0]/result[2], result[1]/result[2] ]
                    

    def run_query(self, input):
        self.cursor = self.connect()
        self.cursor.execute(input)
        return self.conn.commit()

    def return_query_results(self, input):
        self.cursor = self.connect()
        self.cursor.execute(input)
        return self.cursor.fetchall()

    def return_covid_fsa_join_keys(self):
        # Returns 2 table names if they share many values of one of a set of attributes

        limit = "99999" # this should be set as the number of random connections
        offset = "0" # this should be random

        # set of attribute names to join on
        shared_attributes = [        
        #    "'neighborhood'",
        #    "'address_full'",
        #    "'organization_address'",
        #    "'postal'", 
        #    "'postal_code'",
        #    "'ward'"
             "'geometry'"
        ]


        # Get a list of everywhere these columns appear        
        query = "SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND column_name IN (" + ', '.join(shared_attributes) + ") LIMIT " + limit + " OFFSET " + offset
        columns = self.return_query_results( query )

        # For each column per table, try to match it to each subsequent column per table
        for this_i in range(len(columns)) :
            self.this_table = columns[this_i][2]
            this_column = columns[this_i][3] 

            next_table = "covidcasesintoronto"
            next_column = "fsa"
        
            query = "SELECT COALESCE(i.this_count, 0) AS this_count, COALESCE(j.next_count, 0) AS that_count, i.FSA "
            query += " FROM (SELECT COUNT(*) AS this_count, fsa AS FSA FROM " + self.this_table + " b JOIN toronto_fsas t ON ST_Within(b."+ this_column +", t.geom) GROUP BY FSA) i "
            query += " INNER JOIN (SELECT COUNT(*) AS next_count, "+ next_column + " FROM " + next_table + " GROUP BY " + next_column + ") j ON UPPER(i.fsa) = UPPER(j." + next_column  + ") WHERE abs(j.next_count - ( SELECT avg(x.next_count) FROM (SELECT COUNT(*) AS next_count FROM " + next_table + " GROUP BY " + next_column + ") x )) < 500"           
            result = self.return_query_results( query )

            if len(result) > 25:
                try:
                    self.return_best_rsquared(result)
                except Exception as e:
                    print(e)

    def return_best_rsquared(self, input):
        # take x and y, and try to get r2 in a number of ways - return the highest
        # use normal, log, ?remove "outliers"? that are very far from the mean
        # also get stdev

        rs = []
        stds = []
        dats = [
            [[math.log(int(dat[0])) for dat in input], [math.log(int(dat[1])) for dat in input]], 
            [[math.log(int(dat[0])) for dat in input], [int(dat[1]) for dat in input]], 
            [[int(dat[0]) for dat in input], [math.log(int(dat[1])) for dat in input]], 
            [[int(dat[0]) for dat in input], [int(dat[1]) for dat in input]]
        ]

        for dat in dats:
            # x is this, y is that, that is covid rn
            x = dat[0]
            y = dat[1]

            # r^2 values
            res = stats.linregress(x, y)
            rs.append(res.rvalue)

            # stds
            ar = np.array(dat)
            stds.append( np.std(ar) )


        if max(rs)**2 > 0.2:
            print(self.this_table)
            print(f"R-squared: { max(rs)**2 :.4f}")
            #print(f"Standard Dev: { max(stds) :.4f}")
        

        #plt.plot(x, y, 'o', label='original data')
        ##plt.plot(x, res.intercept + res.slope*x, 'r', label='fitted line')
        #plt.legend()
        #plt.show()




    def init_toronto_fsa(self, filepath = "./torontofsa.geojson.txt"):

        try:
            self.run_query( "DROP TABLE toronto_fsas" )
        except:
            pass 
        
        
        with open(filepath, "r") as file: 
            query = "WITH data AS (SELECT '{}'::json AS FC) ".format(file.read())
            query += " SELECT * INTO toronto_fsas FROM ("
            query += " SELECT row_number() OVER () AS gid,"
            query += " ST_GeomFromGeoJSON(feat->>'geometry') AS geom, "
            query += " feat->'properties'->>'CFSAUID' AS fsa "
            query += " FROM ( SELECT json_array_elements(fc->'features') AS feat FROM data) AS f"
            query += " ) x "
        #print(query)
        self.run_query( query ) 

    
    
            



    
if __name__ == '__main__':

    #todclient = torontoopendata.TorontoOpenDataClient()
    psqlclient = PGSQLClient()

    #print(psqlclient.return_query_results("SELECT table_name, column_name, data_type FROM information_schema.columns"))
    psqlclient.return_covid_fsa_join_keys()
    #psqlclient.init_toronto_fsa()