"""This module leverages the APIs to feed data to conbime with the data from Excel
    """
from config import Connect_API_key, ICONA_key
import pandas as pd
import requests
import sys
import json


class IHSAPI():
    """base class for the API operations
    """
    def __init__(self):
        self.headers = {
            'Accept': 'application/json',
            'Authorization': Connect_API_key,
            'User-Agent': 'PostmanRuntime/7.26.8'}
        self.params = {}
    
    
    def getResponse(self, url):
        try:
            response = requests.request(
                "GET", url, headers=self.headers, params=self.params)
            # return response
            reqIsJson = False

            if "application/json" in response.headers.get('content-type'):
                reqIsJson = True

            if response.status_code == 200 and reqIsJson == True:
                return response

            if response.status_code == 200 and reqIsJson == False:
                print("Unsupported content type received : ",
                      response.headers.get('content-type'))
                sys.exit()

            print('Status Code: ' + str(response.status_code))

            if response.status_code == 400:
                print(
                    "The server could not understand your request, check the syntax for your query.")
                print('Error Message: ' + str(response.json()))
            elif response.status_code == 401:
                print("Login failed, please check your user name and password.")
            elif response.status_code == 403:
                print("You are not entitled to this data.")
            elif response.status_code == 404:
                print(
                    "The URL you requested could not be found or you have an invalid view name.")
            elif response.status_code == 500:
                print(
                    "The server encountered an unexpected condition which prevented it from fulfilling the request.")
                print("Error Message: " + str(response.json()))
                print("If this persists, please contact customer care.")
            else:
                print("Error Message: " + str(response.json()))

            sys.exit()

        except Exception as err:
            print("An unexpected error occurred")
            print("Error Message: {0}".format(err))
            sys.exit()

    def column_select_string(self, columns):
        # & % 24select = %22field_name % 22 % 2C % 22prod_status % 22
        str1 = ""
        for column in columns:
            str1 = str1 + column+'%2C'
        str2 = r'&%24select='+str1
        return str2[:-3]

    def get_dataframe_from_api(self, url):
        try:
            response = self.getResponse(url)
            df = pd.DataFrame(json.loads(
                response.text.encode('utf8'))['elements'])
        except:
            df = pd.DataFrame()
        return df

    def get_views_from_api(self):
        view_url = {
            "reservoir": "https://energydataservices.ihsenergy.com/rest/data/v1/international/adm/reservoir/views",
            "reservoir_view_base": "https://energydataservices.ihsenergy.com/rest/data/v1/international/adm/reservoir/views/"
        }
        views = pd.DataFrame(self.getResponse(
            view_url['reservoir']).json())
        data = pd.DataFrame()
        for view in views['name']:
            view_data_temp = pd.DataFrame(self.getResponse(
                view_url['reservoir_view_base']+view).json())
            view_data = pd.DataFrame()
            for row in view_data_temp['Elements']:
                view_data = pd.concat([view_data, pd.DataFrame(
                    row, index=view_data_temp['view_name'])])
            view_data.drop_duplicates()
            data = pd.concat([data, view_data])
        return data.drop_duplicates()


class EandP(IHSAPI):
    """The class for International E&P data (EDIN) 
    """

    def __init__(self) -> None:
        super().__init__()

    def field_data_by_id(self, field_id):
        columns = [
            "fie_id",
            "field_name",
            "country_names",
            "region_name",
            "general_hc_type",
            "hc_type",
            "field_sqkm",
            "prod_status",
            "basin_name",
            "gp_id",
            "political_province",
            "ons_offshore",
            "terrain",
            "porosity_max_val_pct",
            "permeab_max_val_md",
            "latitude_dec_deg",
            "longitude_dec_deg",
            "cur_operator_names",
            "operator_puh_id"
        ]
        url = "https://energydataservices.ihsenergy.com/rest/data/v1/international/adm/eandp/retrieve/field_header?$filter=fie_id=" + \
            str(field_id) + self.column_select_string(columns)

        return self.get_dataframe_from_api(url)

    def reservoir_data_by_id(self, reservoir_id):
        columns = [
            "resv_id",
            "reservoir_unit_name",
            "field_name",
            "parent_lithostrat_unit",
            "lithostrat_unit",
            "lithologies",
            "play_name",
            "depth_ref_elevation_meter",
            "top_depth_meter",
            "top_depth_type",
            "gross_thickn_max_val_meter",
            "gross_thickn_max_val_feet",
            "gross_thickn_qual_unit",
            "net_thickn_max_val_meter",
            "net_thickn_max_val_feet",
            "net_thickn_qual_unit",
            "porosity_min_pct",
            "porosity_avg_pct",
            "porosity_max_pct",
            "permeab_min_md",
            "permeab_avg_md",
            "permeab_max_md",
            "pressure_psi",
            "salinity_max_val_ppm"
        ]
        url = "https://energydataservices.ihsenergy.com/rest/data/v1/international/adm/reservoir/retrieve/field_reservoirs?%24filter=resv_id%3D" + \
            reservoir_id + self.column_select_string(columns)
        return self.get_dataframe_from_api(url)
    
    
class ICONA(IHSAPI):
    def __init__(self):
        self.headers = {
        'accept': 'application/json',
        'icona-auth-key': ICONA_key,
        # 'Authorization': 
        'User-Agent': 'PostmanRuntime/7.26.8'
        }   
        self.params = {}
    
    def get_basics_by_company_id(self, company_id):
        url = r'https://icona-api.etools.ihsenergy.com/api/company?SearchString=C%3A' + str(company_id)
        r = self.getResponse(url)
        company_data = pd.DataFrame(r.json(), index=[company_id])
        return company_data
    
    def get_parent_child_tree_by_id(self, company_id):
        url = r'https://icona-api.etools.ihsenergy.com/api/company/parentchildtree/' + str(company_id)
        r = self.getResponse(url)
        return pd.DataFrame(r.json(), index=[company_id])
