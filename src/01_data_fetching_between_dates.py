import pandas as pd
import numpy as np
import requests
import time
from datetime import date, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

from src.data_fetching_util_electricty_demand import GetDataFromAPI
from src.data_fetching_util_electricty_sources import GetDataFromAPIEnergySource

class DataBetweenDates:

    def __init__(self, dates, api_key) -> list:
        self.dates = dates
        self.api_key = api_key


    def get_data_bw_two_dates(self):



        for date in self.dates:
            
            start = time.time()


            start_date = date + 'T00'
            end_date = date + 'T23'

            p_demand = GetDataFromAPI(start_date, end_date, self.api_key)
            p_source = GetDataFromAPIEnergySource(start_date, end_date, self.api_key)

            data_electricity_demand = p_demand.get_data_from_eia_gov_open_data()
            data_electricity_demand['metric_and_value'] = data_electricity_demand['metric_and_value'].astype(str)
            data_electricity_demand = data_electricity_demand.reset_index(drop=True)


            data_electricity_source = p_source.get_data_from_eia_gov_open_data_energy_source()
#            data['metric_and_value'] = data['metric_and_value'].astype(str)
            data_electricity_source = data_electricity_source.reset_index(drop=True)


            print(data_electricity_demand.shape, data_electricity_source.shape)
            end = time.time()
            print(date, end - start)

#            PATH = Path('data/')
            data_path_electricity_demand = PATH_1/'electricity_data_{}_to_{}.parquet'.format(start_date,end_date)
            data_electricity_demand.to_parquet(data_path_electricity_demand)

            data_path_electricity_source = PATH_2/'electricity_source_{}_to_{}.parquet'.format(start_date,end_date)
            data_electricity_source.to_parquet(data_path_electricity_source)

if __name__ == '__main__':
    PATH_1 = Path('data/electricity_demand')
    PATH_2 = Path('data/electricity_sources')
    print("creating directory structure...")
    (PATH_1).mkdir(exist_ok=True)
    (PATH_2).mkdir(exist_ok=True)



    api_key = 'QRhUgmdXxbYTV8KMhgc2IYaKVUpVtJ9lqo2VKWvv'

    today_date = date.today()
    today_date = today_date.strftime("%Y-%m-%d")

    start_date = '2023-01-01'
    end_date =  '2023-05-01'


    dates = pd.date_range( pd.to_datetime(start_date), pd.to_datetime( today_date )-timedelta(days=1), freq='d')
    dates = dates.astype(str)

    p_data = DataBetweenDates(dates, api_key)
    data = p_data.get_data_bw_two_dates()


