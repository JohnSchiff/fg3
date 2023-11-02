from strategy_functions import *
from utils import *
class StartegyCommon():
    def __init__(self, months=[1], year=2022, dte_min=14,dte_max=31,
                 diff_strike=[0], option_type=1,):
        self.months = [months] if isinstance(months, int) else months or list(range(1, 13))
        self.year = year
        self.dte_min = dte_min
        self.dte_max = dte_max
        self.diff_strike = diff_strike
        self.diff_strike = diff_strike
        self.option_type = option_type
    
    def get_relevant_files(self):
        year_files = get_quotes_by_years(years=self.year)
        files = [file for file in year_files if datetime.strptime(file[-18:-8], '%Y-%m-%d').month in self.months]
        return files 
    
    def filter_by_dte(self, df):
        cond_dte = (df.dte >= self.dte_min) & (df.dte <= self.dte_max)
        df = df.loc[cond_dte]
        
        if len(df) ==0:
            print (f'No data between {self.dte_min} to {self.dte_max}')
        return df
    
class Open_close_strategy(StartegyCommon):
    def __init__(self, time_open='10:00',time_close='17:25'):
        super().__init__()
        self.time_open = pd.to_datetime(time_open, format="%H:%M")       
        self.time_close = pd.to_datetime(time_close, format="%H:%M")   
    
    def proccess_data(self):
        '''
        extracting the relevant data from user's input
        trade in each row with buy/sell  
        '''
        day_before_options_id = None
        l = []  
        # files by months
        files = self.get_relevant_files()
        
        for file in files:
            df = pd.read_parquet(file)
            if day_before_options_id: # When there are options from night before
                cond_in_id_list = (df.mispar_hoze.isin(day_before_options_id))
                cond_time_open = (df['timestamp'].dt.time >= self.time_open.time())
                # filter by conditions
                df_open = df.loc[cond_in_id_list & cond_time_open ]
                df_open = get_morning_quotes(df_open)
                l.append(df_open)    
            # Get relevant id's 
            df = self.filter_by_dte(df)
            df = filter_close_time(df, self.time_close)
            df_close = get_night_quotes(df)
            l.append(df_close)
            day_before_options_id = df_close.mispar_hoze.to_list()
        data = pd.concat(l, axis=0, ignore_index=True, sort=False)
        # Remove rows where no trades for example option at the end of day  without open day after that
        data = remove_non_trade_rows(data).reset_index(drop=True)
        # Arrange the data whrer sell and buy will be in the same row, easier to compute (sum of columns)
        data = sell_buy_in_same_row_df(data, short=False)
        # Add option details such Call/Put, date to exipration
        data = add_options_details(data, self.year)
        # Filter by Call/Put data only
        data = filter_option_type(data,call=False)
        # Filter by days to expiration
        data = filter_by_dte(data,dte_min=self.dte_min,dte_max =self.dte_max)
        return data
    
    def final_result(self, data):
        '''
        Resules of Strategy
        Profit/loss
        '''
        data['profit'] = data['sell'] - data['buy']
        data['accum_profit'] = data.profit.cumsum()
        data['trade_id'] = range(1, len(data) + 1)

        return data
    
    
