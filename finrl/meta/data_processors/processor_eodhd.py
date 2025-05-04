from __future__ import annotations

import datetime
import numpy as np
import pandas as pd
import pandas_market_calendars as tc
import pytz
import os
import requests
import dask.dataframe as dd
#import time
from stockstats import StockDataFrame as Sdf
import time as time_module  # rename it safely
import warnings
from pandas.errors import SettingWithCopyWarning
import gc  # Garbage collector
# warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
import psutil
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

class EodhdProcessor:
    def __init__(self, csv_folder="./"):
        self.csv_folder = csv_folder
        pass


    # 
    def download(self, api_token, dl_vix=True):
        """Fetches data from EODHD API
        Parameters
        ----------
        api_token: token from the EODHD api  (All-in-one plan)
        data_path: path where to save the csv files (of each ticker)

        Returns
        -------
        none
        """

        API_TOKEN = api_token # Replace with your API token

        # Step 1: Get NASDAQ 100 components
        nasdaq_100_ticker_url = f"https://eodhd.com/api/fundamentals/NDX.INDX?api_token={API_TOKEN}&fmt=json&filter=Components"

        response = requests.get(nasdaq_100_ticker_url).json()

        # Extract tickers
        tickers = [data["Code"] for data in response.values()]

        print("{} tickers data to be downloaded".format(len(tickers)))

        # Step 2: Fetch historical minute data for each ticker
        start_date = datetime.datetime(2016, 1, 1)  # Earliest available data for NASDAQ at EODHD
        #end_date = datetime.datetime.now()  # Today
        end_date = datetime.datetime(2016, 1, 5)  # 
        interval = "1m"  # 1-minute interval

        if dl_vix:
            tickers.append("VIX")

        for ticker in tickers:


            all_ticker_data = []
            print(f"Fetching data for {ticker}...")

            start_timestamp = int(time.mktime(start_date.timetuple()))
            end_timestamp = int(time.mktime(end_date.timetuple()))

            while start_timestamp < end_timestamp:
                next_timestamp = start_timestamp + (120 * 24 * 60 * 60)  # 120 days max per request
                if next_timestamp > end_timestamp:
                    next_timestamp = end_timestamp
                
                if ticker == VIX:
                    url = f"https://eodhd.com/api/intraday/VIX.INDX?interval={interval}&from={start_timestamp}&to={next_timestamp}&api_token={API_TOKEN}&fmt=json"
                else:
                    url = f"https://eodhd.com/api/intraday/{ticker}.US?interval={interval}&api_token={API_TOKEN}&fmt=json&from={start_timestamp}&to={next_timestamp}"
                


                response = requests.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        df = pd.DataFrame(data)
                        df["ticker"] = ticker  # Add ticker column
                        all_ticker_data.append(df)
                else:
                    print(f"Error fetching data for {ticker}: {response.text}")

                # Move to next 120-day period
                start_timestamp = next_timestamp
                time.sleep(1)  # Respect API rate limits

            # Step 3: Save the data
            if all_ticker_data:

                final_df = pd.concat(all_ticker_data)
                final_df.to_csv(self.csv_folder+"nasdaq_100_minute_data_"+ticker+".csv", index=False)
                print("Data saved to nasdaq_100_minute_data_"+ticker+".csv")

            else:
                print("No data retrieved.")

        return


    def add_day_column(self):
        """add a Day column to all csv in csv_folder
        Parameters
        ----------

        Returns
        -------
        the max number of days
        """

        # Step 1: First pass to collect all unique dates
        all_dates = []

        max_days = 0

        for filename in os.listdir(self.csv_folder):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.csv_folder, filename)
                df = pd.read_csv(file_path)

                if "datetime" in df.columns:
                    converted = pd.to_datetime(df["datetime"], errors='coerce')
                    dates = converted.dt.date.dropna().tolist()
                    all_dates.extend(dates)

        # Sort and create date index dictionary
        unique_dates = sorted(set(all_dates))
        date_index_dict = {str(date): idx for idx, date in enumerate(unique_dates)}

        max_days = len(date_index_dict)

        print("Date index dictionary created.\n")

        # Step 2: Second pass to update each file with the 'Day' column
        for filename in os.listdir(self.csv_folder):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.csv_folder, filename)
                df = pd.read_csv(file_path)

                if "datetime" in df.columns:
                    # Convert datetime column to datetime objects
                    converted = pd.to_datetime(df["datetime"], errors='coerce')

                    # Map to day indices
                    day_indices = converted.dt.date.map(lambda d: date_index_dict.get(str(d), None))

                    # Add the Day column
                    df["Day"] = day_indices

                    # Save the updated file back
                    df.to_csv(file_path, index=False)

                    print(f"Saved updated file: {filename}")

        return max_days

    def tics_in_more_than_90perc_days(self, max_days):

        sup_to_90=0
        tics_present_in_more_than_90_per = []

        for filename in os.listdir(self.csv_folder):

            print("self.csv_folder {}".format(self.csv_folder))
            print("filename {}".format(filename))


            if filename.endswith('.csv'):
                file_path = os.path.join(self.csv_folder, filename)
                try:
                    df = pd.read_csv(file_path)

                    if 'Day' not in df.columns:
                        print(f"{filename}: 'Day' column not found.")
                        continue

                    # Drop NA values and ensure the column is integer type
                    unique_days = pd.to_numeric(df['Day'], errors='coerce').dropna().astype(int).nunique()
                    percentage = (unique_days / max_days) * 100

                    if percentage > 90:
                        sup_to_90+=1
                        tics_present_in_more_than_90_per.append(str(df['ticker'][0])) 

                    print(f"{filename}: {unique_days} unique days ({percentage:.2f}%)")

                except Exception as e:
                    print(f"{filename}: Error reading file - {e}")

        return tics_present_in_more_than_90_per


    def nber_present_tics_per_day(self, max_days, tics_in_more_than_90perc_days):

        dico_ints = {i:0 for i in range(max_days+1)}
        counter=0

        for filename in os.listdir(self.csv_folder):
            if filename.endswith('.csv'):
                # print(counter)
                # print(self.csv_folder)
                # print(filename)
                
                file_path = os.path.join(self.csv_folder, filename)
                try:
                    #print("in try 0")
                    df = pd.read_csv(file_path)
                    #print("in try 1")
                    if str(df["ticker"][0]) in tics_in_more_than_90perc_days:
                        #print("in try 2")
                        if 'Day' not in df.columns:
                            #print("in try 3")
                            print(f"{filename}: 'Day' column not found.")
                            continue
                        else:
                            #print("in try 4")
                            unique_days = set(df['Day'].tolist())
            
                            for num in unique_days:
                                dico_ints[num] += 1

                except Exception as e:
                    print(f"{filename}: Error reading file - {e}")
                    exit()

                counter += 1

            
        return dico_ints


    def process_after_dl(self):


        # add a day column
        max_days = self.add_day_column()

        # find the tics that are present in more than 90% of the days
        tics_in_more_than_90perc_days = self.tics_in_more_than_90perc_days(max_days)

        # print(tics_in_more_than_90perc_days) # ['WDAY', 'ADP', 'XEL', 'VRTX', 'AAPL', 'VRSK', 'ADBE', 'ADI']

        # create the dict of days (keys) and number of present tics (values)
        dico_ints = self.nber_present_tics_per_day(max_days, tics_in_more_than_90perc_days)

        # for each key (day) if the number of present tics is = tics_in_90%, add the Day id to days_to_keep
        num_of_good_ticks = len(tics_in_more_than_90perc_days)
        days_to_keep = []
        for k,v in dico_ints.items():
            if v == num_of_good_ticks:
                days_to_keep.append(k)



        # loop over each tic CSV and remove non wished days
        df_list = []
        for filename in os.listdir(self.csv_folder):

            if filename.endswith('.csv'):
                print("removed uncomplete days from {}".format(filename))
                file_path = os.path.join(self.csv_folder, filename)
                try:
                    df = pd.read_csv(file_path)
                    if df['ticker'].iloc[0] in tics_in_more_than_90perc_days:
                        filtered_df = df[df['Day'].isin(days_to_keep)]
                        df_list.append(filtered_df.sort_values(by='Day'))
                    #if str(df["ticker"][0]) in tics_present_in_more_than_90_per:
                except Exception as e:
                    print(f"{filename}: Error reading file - {e}")

        
        df.loc[df['ticker'] != 'VIX', 'volume'] = df.loc[df['ticker'] != 'VIX', 'volume'].astype(int)

        df = pd.concat(df_list, ignore_index=True)


        # Reset the Days integers
        unique_days = df['Day'].unique()
        mapping = {old: new for new, old in enumerate(sorted(unique_days))}
        df['Day'] = df['Day'].map(mapping)

        return df, mapping


    def clean_data(self, df, min_24 = True):
        
        df.rename(columns={'ticker': 'tic'}, inplace=True)
        df.rename(columns={'datetime': 'time'}, inplace=True)
        df['time'] = pd.to_datetime(df['time'])
        df = df[["time", "open", "high", "low", "close", "volume", "tic", "Day"]]
        # remove 16:00 data
        #df.drop(df[df["time"].astype(str).str.endswith("16:00:00")].index, inplace=True)
        df = df.drop(df[df["time"].astype(str).str.endswith("16:00:00")].index)
        df.sort_values(by=["tic", "time"], inplace=True)
        df.reset_index(drop=True, inplace=True)


        tics = df["tic"].unique()
        days = df["Day"].unique()

  
        start_time = df['time'].min().replace(hour=0, minute=0, second=0)
        end_time = df['time'].max().replace(hour=23, minute=59, second=0)
        time_range = pd.date_range(start=start_time, end=end_time, freq='min') 


        df['date_only'] = df['time'].dt.date
        days_dict = dict(zip(df['Day'], df['date_only']))
        all_rows = []
        for day_num, date in days_dict.items():
            minute_times = pd.date_range(start=pd.Timestamp(date), periods=1440, freq='min')
            for time in minute_times:
                all_rows.append({'time': time, 'day': day_num})
        df.drop(columns=['date_only'], inplace=True)
        df_minute_lvl = pd.DataFrame(all_rows)

        process = psutil.Process(os.getpid())
        print(f"Memory usage: {process.memory_info().rss / 1024 ** 2:.2f} MB") # 7Go



        df_size_bytes = df.memory_usage(deep=True).sum()
        total_gb = df_size_bytes / (1024 ** 3)
        print(f"Estimated memory needed (for concat only): {total_gb:.2f} GB")


        grouped = df.groupby(['Day', 'tic'])


        process = psutil.Process(os.getpid())
        print(f"Memory usage1: {process.memory_info().rss / 1024 ** 2:.2f} MB") # 7Go


        missing_times_list = []


        total_total = 0

        # Instead of collecting all in missing_times_list and concatenating at the end...

        for cc, tic in enumerate(tics):
            print("Adding Missing Rows for tic {}, {}/{}".format(tic, cc, len(tics)))
            
            for day in days:
                subset = grouped.get_group((day, tic))
                times_for_this_tic_and_day = subset['time']
                times_for_this_tic_and_day = pd.to_datetime(times_for_this_tic_and_day)

                if min_24:
                    filtered_minute_df = df_minute_lvl.loc[df_minute_lvl['day'] == day].copy()
                    filtered_minute_df = filtered_minute_df.drop(columns=['day'])
                    missing_times = filtered_minute_df[~filtered_minute_df['time'].isin(times_for_this_tic_and_day)].copy()
                else:
                    existing_time_values = df[df['Day'] == day]['time'].unique()
                    existing_time_df = pd.DataFrame({'time': pd.to_datetime(existing_time_values)})
                    missing_times = existing_time_df[~existing_time_df['time'].isin(times_for_this_tic_and_day)]

                missing_times.loc[:, 'open'] = np.nan
                missing_times.loc[:, 'high'] = np.nan
                missing_times.loc[:, 'low'] = np.nan
                missing_times.loc[:, 'close'] = np.nan
                missing_times.loc[:, 'volume'] = np.nan
                missing_times.loc[:, 'tic'] = tic
                missing_times.loc[:, 'Day'] = day
                missing_times = missing_times.astype({
                    'open': 'float64',
                    'high': 'float64',
                    'low': 'float64',
                    'close': 'float64',
                    'volume': 'float64',
                    'tic': 'object',
                    'Day': 'int64'
                })


                if not missing_times.empty:
                    missing_times_list.append(missing_times)


        df = pd.concat([df] + missing_times_list, ignore_index=True) 
        print("finally")

        df.sort_values(["tic", "time"], inplace=True)

        print("sorted")
        
        # Replace all 0 volume with a Nan  (to allow for ffill and bfill to work)
        df.loc[df['volume'] == 0, 'volume'] = np.nan

        ## FILLING THE MISSING ROWS
        for tic in tics:
            print("Filling Missing Rows for tic {}".format(tic))
            cols_to_ffill = ['close', 'open', 'high', 'low', 'volume']
            df.loc[df['tic'] == tic, cols_to_ffill] = (
                df.loc[df['tic'] == tic, cols_to_ffill].ffill().bfill()
            )

        df.reset_index(drop=True, inplace=True)

        print("returning")

        return df


    # process = psutil.Process(os.getpid())
    # print(f"Memory usage: {process.memory_info().rss / 1024 ** 2:.2f} MB") # 7Go

    # def add_technical_indicator(
    #     self,
    #     df,
    #     tech_indicator_list=[
    #         "macd",
    #         "boll_ub",
    #         "boll_lb",
    #         "rsi_30",
    #         "dx_30",
    #         "close_30_sma",
    #         "close_60_sma",
    #     ],
    # ):
    #     df = df.rename(columns={"time": "date"})
    #     df = df.sort_values(by=["tic", "date"])

    #     stock = Sdf.retype(df)

    #     # Calculate and store indicators first
    #     for indicator in tech_indicator_list:
    #         print(f"Calculating indicator: {indicator}")
    #         stock[indicator] = stock[indicator]  # store indicator results in stock
    #         stock[indicator] = pd.to_numeric(stock[indicator], downcast="float")
    #         print(stock[indicator])

    #     unique_ticker = df["tic"].unique()
    #     grouped_stock = stock.groupby('tic')
    #     grouped_df = df.groupby('tic')

    #     for indicator in tech_indicator_list:
    #         print(f"Processing indicator {indicator}")
    #         indicator_data = []



    #         process = psutil.Process(os.getpid())
    #         print(f"Memory usage: {process.memory_info().rss / 1024 ** 2:.2f} MB") # 7Go

    #         for tic in unique_ticker:

                
    #             if tic in grouped_stock.groups:
    #                 temp = grouped_stock.get_group(tic)[[indicator]].copy()
    #                 temp["tic"] = tic
    #                 temp["date"] = grouped_df.get_group(tic)["date"].values

    #                 indicator_data.append(temp)

    #                 del temp

    #         indicator_df = pd.concat(indicator_data, axis=0, ignore_index=True)
    #         df = df.merge(
    #             indicator_df[["tic", "date", indicator]],
    #             on=["tic", "date"],
    #             how="left"
    #         )

    #         print("indicator_df.dtypes")
    #         print(indicator_df.dtypes)
    #         exit()


    #         del indicator_data
    #         del indicator_df

    #     df = df.sort_values(by=["date", "tic"])
    #     print("Successfully added technical indicators")
    #     return df


    def create_indicators(
        self,
        df,
        output_dir="indicators",
        tech_indicator_list=[
            "macd",
            "boll_ub",
            "boll_lb",
            "rsi_30",
            "dx_30",
            "close_30_sma",
            "close_60_sma",
        ],
    ):
        os.makedirs(output_dir, exist_ok=True)
        df = df.rename(columns={"time": "date"})
        df = df.sort_values(by=["tic", "date"])
        stock = Sdf.retype(df.copy())
        unique_ticker = stock.tic.unique()

        for indicator in tech_indicator_list:
            print(f"Generating indicator: {indicator}")
            indicator_data = []

            for tic in unique_ticker:
                temp_df = pd.DataFrame(stock[stock.tic == tic][indicator])
                temp_df["tic"] = tic
                temp_df["date"] = df[df.tic == tic]["date"].values
                indicator_data.append(temp_df)

            indicator_df = pd.concat(indicator_data, ignore_index=True)
            path = os.path.join(output_dir, f"{indicator}.parquet")
            indicator_df.to_parquet(path, index=False)
            print(f"Saved: {path}")

        return



    def add_technical_indicators(
        self,
        df,
        input_dir="indicators",
        tech_indicator_list=[
            "macd",
            "boll_ub",
            "boll_lb",
            "rsi_30",
            "dx_30",
            "close_30_sma",
            "close_60_sma",
        ],
    ):
        df = df.rename(columns={"time": "date"}) if "time" in df.columns else df
        df = df.sort_values(by=["tic", "date"])

        print("df.dtypes1")
        print(df.dtypes)

        for indicator in tech_indicator_list:
            path = os.path.join(input_dir, f"{indicator}.parquet")
            print(f"Merging indicator from: {path}")
            if os.path.exists(path):
                indicator_df = pd.read_parquet(path)

                indicator_df[indicator] = pd.to_numeric(indicator_df[indicator], downcast="float")
                indicator_df["tic"] = indicator_df["tic"].astype("category")

                df = df.merge(indicator_df, on=["tic", "date"], how="left")
                print(df.head(5))
  

            else:
                print(f"Warning: {path} not found. Skipping.")
        

        print(df.head())
        df = df.sort_values(by=["date", "tic"])
        print("Successfully merged all technical indicators.")
        print(df.head())
        return df



    def calculate_turbulence(self, df, time_period=252):
        print("ICI1")
        df_price_pivot = df.pivot(index="date", columns="tic", values="close").pct_change()

        unique_dates = df_price_pivot.index
        turbulence_index = np.zeros(len(unique_dates))
        turbulence_index[:time_period] = 0
        print("ICI2")
        # Pre-compute column-wise non-NaN counts
        non_nan_counts = df_price_pivot.notna().sum()
        len_unique_date = len(unique_dates)
        for i in range(time_period, len(unique_dates)):

            print(str((i / len_unique_date) * 100)) # 0.02
            current_date = unique_dates[i]
            past_window = df_price_pivot.iloc[i - time_period:i]

            # Drop columns (tickers) with too many missing values
            min_non_nan = past_window.notna().sum().min()
            filtered = past_window.loc[:, past_window.notna().sum() >= min_non_nan].dropna(axis=1)

            if filtered.shape[1] < 2:
                turbulence_index[i] = 0
                continue

            cov_matrix = filtered.cov().values
            try:
                inv_cov = np.linalg.pinv(cov_matrix)
            except np.linalg.LinAlgError:
                turbulence_index[i] = 0
                continue

            current_row = df_price_pivot.loc[current_date, filtered.columns]
            mean_row = filtered.mean()
            diff = (current_row - mean_row).values.reshape(1, -1)

            temp = diff @ inv_cov @ diff.T
            temp_value = temp[0, 0] if temp > 0 else 0
            turbulence_index[i] = temp_value if i - time_period > 2 else 0

        return pd.DataFrame({
            "date": unique_dates,
            "turbulence": turbulence_index
        })







    # Global shared data (read-only)
    _shared_price_pivot = None
    _shared_unique_dates = None
    _shared_time_period = None

    def _init_worker(self, price_pivot, unique_dates, time_period):
        global _shared_price_pivot, _shared_unique_dates, _shared_time_period
        _shared_price_pivot = price_pivot
        _shared_unique_dates = unique_dates
        _shared_time_period = time_period

    def _compute_turbulence(self, i):
        current_date = _shared_unique_dates[i]
        df_price_pivot = _shared_price_pivot
        time_period = _shared_time_period

        hist_prices = df_price_pivot.iloc[i - time_period:i]
        current_price = df_price_pivot.loc[current_date]

        # Drop columns with too many NaNs
        min_non_nan = hist_prices.notna().sum().min()
        filtered = hist_prices.loc[:, hist_prices.notna().sum() >= min_non_nan].dropna(axis=1)

        if filtered.shape[1] < 2:
            return 0.0

        cov_matrix = filtered.cov().values
        try:
            inv_cov = np.linalg.pinv(cov_matrix)
        except np.linalg.LinAlgError:
            return 0.0

        current_diff = (current_price[filtered.columns] - filtered.mean()).values.reshape(1, -1)
        turbulence = current_diff @ inv_cov @ current_diff.T

        turbulence_value = turbulence[0, 0] if turbulence > 0 else 0.0
        if i - time_period <= 2:
            return 0.0
        return turbulence_value

    def calculate_turbulence_parallel(self, df, time_period=252, num_processes=None):

        print("IN 0")

        df_price_pivot = df.pivot(index="date", columns="tic", values="close").pct_change()
        unique_dates = df_price_pivot.index
        print("IN 1")
        indices = list(range(time_period, len(unique_dates)))
        print("IN 2")
        if num_processes is None:
            num_processes = min(cpu_count(), 8)

        print("IN 3")

        with Pool(
            processes=num_processes,
            initializer=self._init_worker,
            initargs=(df_price_pivot, unique_dates, time_period)
        ) as pool:
            turbulence_values = list(
                tqdm(pool.map(self._compute_turbulence, indices), total=len(indices))
            )
        print("IN 4")
        # Build turbulence series
        turbulence_index = [0.0] * time_period + turbulence_values
        return pd.DataFrame({"date": unique_dates, "turbulence": turbulence_index})






    def add_turbulence(self, df, time_period=252):
        """
        add turbulence index from a precalcualted dataframe
        :param data: (df) pandas dataframe
        :return: (df) pandas dataframe
        """
        print("ICI0")
        turbulence_index = self.calculate_turbulence_parallel(df, time_period=time_period)
        df = df.merge(turbulence_index, on="date")
        df = df.sort_values(["date", "tic"]).reset_index(drop=True)
        return df