import pandas as pd
import numpy as np
import requests
from pathlib import Path
from datetime import date, timedelta
import warnings
warnings.filterwarnings('ignore')




class GetDataFromAPI():

    def __init__(self, start_date, end_date, api_key) -> str:
        self.start_date = start_date
        self.end_date = end_date
        self.api_key = api_key


    def get_data_from_eia_gov_open_data(self):
        '''
        This pulls data from the https://www.eia.gov/opendata/ API 
        '''

        # getting first 5000 rows since its the limit set by API
        url_1 = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=hourly&data[0]=value&start={}&end={}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key={}'.format(self.start_date,self.end_date,self.api_key)
        response = requests.get(url_1)
        data_1_to_5000_rows = pd.DataFrame(response.json()['response']['data'])

        # getting the reamaining rows 
        url_2 = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=hourly&data[0]=value&start={}&end={}&sort[0][column]=period&sort[0][direction]=desc&offset=5000&length=5000&api_key={}'.format(self.start_date,self.end_date,self.api_key)
        response = requests.get(url_2)
        data_5000_to_end_rows = pd.DataFrame(response.json()['response']['data'])

        # concatenating the dataframes
        data = pd.concat([data_1_to_5000_rows, data_5000_to_end_rows])

        # perfroming groupby to get the required data for a particular time-period and thr corresponding utility station (respondent)
        data = data.groupby(['period', 'respondent', 'respondent-name']).agg(lambda x: list(x))

        # resetting index
        data= data.reset_index()

        # combining the energy metric ( Day-ahead demand forecast, Demand, Net generation, Total interchange ) with its value
        data['metric_and_value'] = [list(x) for x in map(zip, data['type-name'], data['value'])]

        # taking only those data points where all the 4 metric values are present
        data['num'] = data['value'].apply(lambda x : len(x))
        data = data[data['num'] == 4]
        
        # sorting the tuples (metric name, value) by the metric name
        data['metric_and_value'] = data['metric_and_value'].apply(lambda x : sorted(x))

        # assigning the values to the new metric columns
        data['Day-ahead demand forecast'] = data['metric_and_value'].apply(lambda x : x[0][1])
        data['Demand'] = data['metric_and_value'].apply(lambda x : x[1][1])
        data['Net generation'] = data['metric_and_value'].apply(lambda x : x[2][1])
        data['Total interchange'] = data['metric_and_value'].apply(lambda x : x[3][1])

        # extracting only those data points where all the 4 different metrics are present
        arg_t = ('Day-ahead demand forecast', 'Demand', 'Net generation', 'Total interchange')
        data = data[data['metric_and_value'].apply(lambda x : list(zip(*x))[0]) == arg_t]


        return data
    

if __name__ == '__main__':
    PATH = Path('data/electricity_demand')
    print("creating directory structure...")
    (PATH).mkdir(exist_ok=True)

    start_date = '2016-02-23T00'
    end_date = '2016-02-23T23'
    api_key = 'QRhUgmdXxbYTV8KMhgc2IYaKVUpVtJ9lqo2VKWvv'
    get_data_class = GetDataFromAPI(start_date, end_date, api_key)
    data = get_data_class.get_data_from_eia_gov_open_data()
    data['metric_and_value'] = data['metric_and_value'].astype(str)  ## error in saving to parquet version in the newer version so need to convert this column to str

    data_path = PATH/'electricity_data_{}_{}.parquet'.format(start_date,end_date)
    data.to_parquet(data_path)
