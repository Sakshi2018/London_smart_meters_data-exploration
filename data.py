import os
import pandas as pd
from typing import NamedTuple

class DataSource:
    root_path = ""
    def __init__(self, path, names, dates=[], **options):
        self.path = path
        self.names = names
        self.dates = dates
        self.options = options

    def load_frame(self) -> pd.DataFrame:
        return pd.read_csv(os.path.join(self.root_path, self.path),
                           names=self.names, parse_dates=self.dates,
                           **self.options)

class SplitDataSource:
    root_path = ""
    def __init__(self, path, names, dates=[], **options):
        self.path = path
        self.names = names
        self.dates = dates
        self.options = options

    def load_frame(self, block_n: int) -> pd.DataFrame:
        return pd.read_csv(os.path.join(self.root_path, self.path.format(block_n)),
                           names=self.names, parse_dates=self.dates,
                           **self.options)
    
    def block_count(self):
        return len(os.listdir(os.path.dirname(os.path.join(self.root_path, self.path))))

def load_data(root_path: str):
    DataSource.root_path = root_path
    SplitDataSource.root_path = root_path

    all_data = NamedTuple('Data')

    '''
    power consumption halfhourly block
        lcl_id: household id
        datetime: tstp
        energy: energy consumption (kWh/hh)
    '''
    all_data.halfhourly_data = SplitDataSource(
        'halfhourly_dataset/block_{}.csv',
        names=['lcl_id', 'datetime', 'energy'],
        dates=['datetime'],
        header=0
    )

    '''
    power consumption daily
        lcl_id: household id
        day: date
        energy mean and stats
    '''
    all_data.daily_data = SplitDataSource(
        'daily_dataset/block_{}.csv',
        names=[
            'lcl_id', 'day', 'energy_median', 'energy_mean', 'energy_max',
            'energy_count', 'energy_std', 'energy_sum', 'energy_min'
        ],
        dates=['day'],
        header=0
    )

    '''
    acorn data
        main_category: Main category
        sub_category: subcategory
        reference: ref value
        acorn-a: acorn category
        ...
        acorn-q
    '''
    all_data.acorn_details = DataSource(
        'acorn_details.csv',
        names=['main_cat', 'sub_cat', 'reference'] + \
                ['acorn-' + chr(x) for x in range(ord('a'), ord('q')+1)],
        encoding="ISO-8859-1",
        header=0
    )


    '''
    lcl_id: household id
    tariff_type: Std(fixed tariff) or ToU(Time of Use)
    acorn: acorn group
    acorn_grouped:
    block_file: block file
    '''
    all_data.informations_households = DataSource(
        'informations_households.csv',
        names=['lcl_id', 'tariff_type', 'acorn', 'acorn_grouped', 'block_file'],
        header=0
    )

    '''
    date: date
    holiday_name
    '''
    all_data.holidays = DataSource(
        'uk_bank_holidays.csv',
        names=['date', 'holiday_name'],
        header=0
    )

    all_data.weather_daily = DataSource(
        'weather_daily_darksky.csv',
        names=[
            "max_temp", "max_temp_datetime", "wind_bearing", "icon", "dew_point",
            "min_temp_datetime", "cloud_cover", "wind_speed", "pressure",
            "apparent_min_temp_time", "apparent_high_temp", "precip_type",
            "visibility", "humidity", "apparent_high_temp_time", "apparent_low_temp",
            "apparent_max_temp", "uv_index", "time", "sunset_time", "low_temp",
            "min_temp", "high_temp", "sunrise_time", "high_temp_time", "uv_index_time", 
            "summary", "low_temp_time", "apparent_min_temp", "apparent_max_temp_time", 
            "apparent_low_temp_time", "moon_phase"
        ],
        header=0
    )

    all_data.weather_hourly = DataSource(
        'weather_hourly_darksky.csv',
        names=[
            'visibility', 'wind_bearing', 'temp', 'datetime', 'dew_point', 'pressure',
            'apparent_temp', 'wind_speed', 'precip_type', 'icon', 'humidity', 'summary'
        ],
        dates=['datetime'],
        header=0
    )

    return all_data


if __name__ == '__main__':
    data = load_data("~/projects/hack-a-gig/power-consumption-forecast/data")
