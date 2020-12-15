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

        for package in self.package_list:
            resources = json.loads( requests.get(self.base_url + "package_show?id=" + package).text)["result"]["resources"]
            for resource in resources:
                if resource["datastore_active"] == True:
                    fields = json.loads( requests.get( self.base_url + "datastore_search?id=" + resource["id"] ).text)["result"]["fields"]
                    print(fields)


if __name__ == "__main__":

    TorontoOpenDataClient()