#!python3
# runner.py - placeholder script to run ETLs from source systems into gulliver pgsql

import logging
import torontoopendata
import pgsql

# Init logging
logging.basicConfig(format='%(levelname)s-%(asctime)s: %(message)s', level=logging.INFO, filename="runner.log")

todclient = torontoopendata.TorontoOpenDataClient()
psqlclient = pgsql.PGSQLClient()

for package_name in todclient.package_list:
    if package_name not in  ["field-information-reports", "highrise-residential-fire-inspection-results"]: #field info reports too big, highrise fire has duplicate 'latitude' column
        logging.info("Requesting data for: " + package_name)
        package = todclient.return_package_data(package_name)
        if len(package["records"]) > 0:
            try:

                
                logging.info("Collected records for " + package_name)
                
                psqlclient.create_table(package)
                logging.info("Created table for " + package_name)
                psqlclient.trunc_and_load_table(package)
                logging.info("Trunc and Loaded records for " + package_name)

                logging.info("Completed ETL for " + package_name)

            except Exception as e:
                logging.error("Failure to complete ETL for: " + package_name)
                logging.error(e)