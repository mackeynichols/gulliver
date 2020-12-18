#!python3
# base.py - base classes for gulliver 

import requests
import json


class TorontoOpenDataClient:
    # class to hit Toronto Open Data portal API programmatically
    def __init__(self): 
        
        # get package (endpoint) list 
        self.base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        self.package_list_url = self.base_url + "package_list"
        self.package_list = json.loads( requests.get( self.package_list_url ).text)["result"]


    def request(self, url):
        # we make lots of requests w json responses, so this is useful
        return json.loads( requests.get( url ).text )


    def return_package_list(self):
        # get the list of all packages
        self.base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        self.package_list_url = self.base_url + "package_list"
        
        return request(package_list_url)["result"]

        
        
        # get a single package name, which we use as an input for our final call
        #package = "bicycle-thefts"
        #print(package)

    def return_package_data(self, package):
        # grab the fields and records for a given package 
        package_url = self.base_url + "package_show?id=" + package
        resources = self.request(package_url)["result"]["resources"]

        for resource in resources:
            if resource["datastore_active"] == True:
                fields = self.request(self.base_url + "datastore_search?id=" + resource["id"])["result"]["fields"]
                records = self.request(self.base_url + "datastore_search?id=" + resource["id"])["result"]["records"]
                resource_id = resource["id"]
            else:
                fields = []
                records = []
                resource_id = resource["id"]
                
        return  {
                'fields': fields, 
                'records': records,
                'resource': resource_id,
                'package_url': package_url
                }
            

        


if __name__ == "__main__":
    print(TorontoOpenDataClient().return_package_data("bicycle-thefts"))