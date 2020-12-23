#!python3
# base.py - base classes for gulliver 

import requests
import json
import time
import logging


class TorontoOpenDataClient:
    # class to hit Toronto Open Data portal API programmatically
    def __init__(self): 

        # init logging
        logging.basicConfig(format='%(levelname)s-%(asctime)s: %(message)s', level=logging.INFO, filename="torontoopendata.log")
        
        # get package (endpoint) list 
        self.base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/"
        self.package_list_url = self.base_url + "api/3/action/package_list"
        self.package_list = json.loads( requests.get( self.package_list_url ).text)["result"]


    def request(self, url):
        # we make lots of requests w json responses, so this is useful
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
            "Accept-Encoding": "*",
            "Connection": "keep-alive"
        }

        return json.loads( requests.get( url, headers = headers ).text )


 
        
        
        # get a single package name, which we use as an input for our final call
        #package = "bicycle-thefts"
        #print(package)

    def return_package_data(self, package):
        # grab the fields and records for a given package 
        package_url = self.base_url + "api/3/action/package_show?id=" + package
        resources = self.request(package_url)["result"]["resources"]
        records = []

        for resource in resources:
            if resource["datastore_active"] == True:

                time.sleep(60)
                

                resource_id = resource["id"]
                resource_url = self.base_url + "api/3/action/datastore_search?id=" + resource["id"]
                response = self.request(resource_url)
                fields = response["result"]["fields"]
                records += response["result"]["records"]
                
                while len(response["result"]["records"]) > 0:
                    
                    response = self.request( self.base_url + response["result"]["_links"]["next"] )
                    records += response["result"]["records"] 

                    
                    
            else:
                fields = []
                records = []
                resource_id = ""

        if len(records) > 0:
            logging.info("Returning records for " + package + ": " + str(len(records)))
                
        return  {
                    'fields': fields, 
                    'records': records,
                    'package': package,
                    'resource_id': resource_id,
                    'package_url': package_url
                }
            

        


if __name__ == "__main__":
    print(TorontoOpenDataClient().return_package_data("bicycle-thefts"))