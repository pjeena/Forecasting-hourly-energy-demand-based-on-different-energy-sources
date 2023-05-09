import pandas as pd
import numpy as np
import requests
from pathlib import Path
from datetime import date, timedelta
import warnings
warnings.filterwarnings('ignore')




class GetDataFromAPIEnergySource():

    def __init__(self, start_date, end_date, api_key) -> str:
        self.start_date = start_date
        self.end_date = end_date
        self.api_key = api_key


    def get_data_from_eia_gov_open_data_energy_source(self):
        '''
        This pulls data from the https://www.eia.gov/opendata/ API 
        '''

        # getting the first 5000 rows
        url_1 = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?frequency=hourly&data[0]=value&start={}&end={}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key={}'.format(self.start_date,self.end_date,self.api_key)
        response = requests.get(url_1)
        data_1_to_5000_rows = pd.DataFrame(response.json()['response']['data'])

        # getting the reamaining rows 
        url_2 = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?frequency=hourly&data[0]=value&start={}&end={}&sort[0][column]=period&sort[0][direction]=desc&offset=5000&length=5000&api_key={}'.format(self.start_date,self.end_date,self.api_key)
        response = requests.get(url_2)
        data_5000_to_end_rows = pd.DataFrame(response.json()['response']['data'])

        # concatenating the dataframes
        data = pd.concat([data_1_to_5000_rows, data_5000_to_end_rows])

        # perfroming groupby to get the required data for a particular time-period and thr corresponding utility station (respondent)
        data = data.groupby(['period', 'respondent', 'respondent-name']).agg(lambda x: list(x))

        # resetting index
        data= data.reset_index()

        # combining the energy sources ( 'Natural gas','Coal','Wind', 'Other','Hydro','Solar','Petroleum','Nuclear' ) with its value
        data['source_and_value'] = [list(x) for x in map(zip, data['type-name'], data['value'])]

        # taking only those data points where all the 4 metric values are present
        data['source_and_value'] = data['source_and_value'] .apply(lambda x : dict(x))


        # assigning the values to the energy source columns
        data['Natural gas'] = data['source_and_value'].apply(lambda x : x['Natural gas'] if 'Natural gas' in x.keys() else np.nan)
        data['Coal'] = data['source_and_value'].apply(lambda x : x['Coal'] if 'Coal' in x.keys() else np.nan)
        data['Wind'] = data['source_and_value'].apply(lambda x : x['Wind'] if 'Wind' in x.keys() else np.nan)
        data['Hydro'] = data['source_and_value'].apply(lambda x : x['Hydro'] if 'Hydro' in x.keys() else np.nan)
        data['Solar'] = data['source_and_value'].apply(lambda x : x['Solar'] if 'Solar' in x.keys() else np.nan)
        data['Petroleum'] = data['source_and_value'].apply(lambda x : x['Petroleum'] if 'Petroleum' in x.keys() else np.nan)
        data['Nuclear'] = data['source_and_value'].apply(lambda x : x['Nuclear'] if 'Nuclear' in x.keys() else np.nan)
        data['Other'] = data['source_and_value'].apply(lambda x : x['Other'] if 'Other' in x.keys() else np.nan)

        return data
    


if __name__ == '__main__':
        PATH = Path('data/electricity_sources')
        print("creating directory structure...")
        (PATH).mkdir(exist_ok=True)

        start_date = '2023-01-01T00'
        end_date = '2023-01-01T23'
        api_key = 'QRhUgmdXxbYTV8KMhgc2IYaKVUpVtJ9lqo2VKWvv'
        get_data_class = GetDataFromAPIEnergySource(start_date,end_date,api_key)
        data = get_data_class.get_data_from_eia_gov_open_data_energy_source()
#        data['metric_and_value'] = data['metric_and_value'].astype(str)  ## error in saving to parquet version in the newer version so need to convert this column to str

        data_path = PATH/'electricity_source_{}_{}.parquet'.format(start_date,end_date)
        data.to_parquet(data_path)
