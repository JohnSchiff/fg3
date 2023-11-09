from utils import *
class StartegyCommon():
    def __init__(self, months=[1], years=[2021], dte_min=14,dte_max=31,
                 diff_strike=[0], option_type=1):
        self.months = [months] if isinstance(months, int) else months or list(range(1, 13))
        self.diff_strike = [diff_strike] if isinstance(diff_strike, int) else diff_strike or [0]
        self.year = [years] if isinstance(years, int) else years
        self.dte_min = dte_min
        self.dte_max = dte_max
        self.option_type = option_type
    
    def get_relevant_files(self):
        year_files = get_quotes_by_years(years=self.year)
        files = [file for file in year_files if datetime.strptime(file[-18:-8], '%Y-%m-%d').month in self.months]
        return files 
    
    def filter_by_option_type(self, df):
        df = df.loc[df.option_type == self.option_type]
        return df
    
    def filter_by_dte(self, df):
        cond_dte = (df.dte >= self.dte_min) & (df.dte <= self.dte_max)
        df = df.loc[cond_dte]
        
        if len(df) ==0:
           pass
        return df
    
    def filter_by_diff_strike(self, df):
        cond_diff_strike = (df.diff_strike.isin(self.diff_strike))
        df = df.loc[cond_diff_strike]
        
        if len(df) ==0:
            pass
        return df
    
class Open_close_strategy(StartegyCommon):
    def __init__(self, time_open='10:00',time_close='17:25',strategy_type='Short', weekend=None,**kwargs):
        super().__init__(**kwargs)
        self.time_open = pd.to_datetime(time_open, format="%H:%M")       
        self.time_close = pd.to_datetime(time_close, format="%H:%M")   
        self.strategy_type = strategy_type
        self.weekend = weekend
    def get_morning_quotes(self, df):
        df = df.groupby('mispar_hoze').head(1)
        df['open'] = 1
        return df

    def get_night_quotes(self, df):
        df = df.groupby('mispar_hoze').tail(1)
        df['open'] = 0
        return df
         
    def filter_close_time(self, df):
        #Protection for time close not too above 
        # Check if the DataFrame is empty first
        if df.empty:
            latest_time_in_df = self.time_close.time()  # or whatever default you want to use when df is empty
        else:
            latest_time_in_df = df['timestamp'].dt.time.max()

        # Proceed with the comparison
        time_close = min(latest_time_in_df, self.time_close.time())

        df = df.loc[df['timestamp'].dt.time < time_close]
        return df
    
    def remove_non_trade_rows(self, df):
        '''
        Remove rows which not followd by or following a trade
        '''
        df = df[['mispar_hoze','option_type','dte','p1_Bid','p1_Ask','timestamp','open','diff_strike','ta35']]
        df.sort_values(by=['mispar_hoze', 'timestamp'], inplace=True)
        # Filter close rows which has same id After, open is 0 and open after that is 1
        close_rows = ((df['mispar_hoze'] == df['mispar_hoze'].shift(-1))
                    & (df.open == 0) & (df.open.shift(-1) == 1))
        # Filter open rows which has same id Before, open is 1 and open after that is 0
        open_rows = ((df['mispar_hoze'] == df['mispar_hoze'].shift(1))
                    & (df.open == 1) & (df.open.shift(1) == 0))
        df = df.loc[close_rows | open_rows]
        return df
    
    def sell_buy_in_same_row_df(self, df):
        # asssign trade id for each trade
        df['trade_id'] = (df.index // 2) + 1
        if self.strategy_type == 'Short':
            sell = 'p1_Bid'
            buy = 'p1_Ask'
            cols = ['trade_id', 'mispar_hoze','dte','option_type','diff_strike', 'sell',
                    'buy', 'timestamp_close', 'timestamp_open','ta35_close','ta35_open']
        else:
            sell = 'p1_Ask'
            buy  = 'p1_Bid'
            cols = ['trade_id', 'mispar_hoze','dte','option_type','diff_strike', 'buy',
                    'sell', 'timestamp_close', 'timestamp_open','ta35_close','ta35_open']
        # Define custom aggregation functions
        agg_funcs = {
            'mispar_hoze': 'first',
            'dte'        : 'first',
            'option_type': 'first',
            'diff_strike': 'first',
            sell         : 'first',
            buy          : 'last',
            'timestamp'  : ['first', 'last'],
            'ta35'       : ['first', 'last'] } 
        # Aggregate both first and last timestamps
        # Group by 'trade_id' and apply custom aggregation functions
        result_df = df.groupby('trade_id', as_index=False).agg(agg_funcs)
        # Rename the resulting columns for clarity
        result_df.columns = cols
        result_df.sort_values(by='timestamp_close',inplace=True)
        return result_df
    
    def final_result(self, df):
        '''
        Resules of Strategy
        Profit/loss
        '''
        df['trade_id'] = range(1, len(df) + 1)
        df.reset_index(drop=True, inplace=True)
        df['profit'] = df['sell'] - df['buy']
        return df
    
    def group_and_sum(self, df, group_by='year'):
        if group_by == 'year':
            result = df.groupby(df['timestamp_close'].dt.year)['profit'].sum().reset_index()
            result.columns = ['Year', 'Total']
        elif group_by == 'month':
            result = df.groupby(df['timestamp_close'].dt.month)['profit'].sum().reset_index()
            result.columns = ['Month', 'Total']
        elif group_by == 'diff_strike':
            result = df.groupby(df['diff_strike'])['profit'].sum().reset_index()
            result.columns = ['Diff_Strike', 'Total']
        else:
            raise ValueError("Invalid group_by parameter. Use 'year' or 'month'.")
        return result
    
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
            print(f'*****************{file}*********************')
            df = pd.read_parquet(file)
            if day_before_options_id: # When there are options from night before
                cond_in_id_list = (df.mispar_hoze.isin(day_before_options_id))
                cond_time_open = (df['timestamp'].dt.time >= self.time_open.time())
                # Filter by conditions
                df_open = df.loc[cond_in_id_list & cond_time_open ]
                df_open = self.get_morning_quotes(df_open)
                l.append(df_open)    
            # Filter by days to expiration
            df = self.filter_by_option_type(df)
            df = self.filter_by_dte(df)
            df = self.filter_close_time(df)
            df_close = self.get_night_quotes(df)
            df_close = self.filter_by_diff_strike(df_close)
            l.append(df_close)
            # Get relevant id's #
            day_before_options_id = df_close.mispar_hoze.to_list()
        data = pd.concat(l, axis=0, ignore_index=True, sort=False)
        # Remove rows where no trades for example option at the end of day  without open day after that
        data = self.remove_non_trade_rows(data).reset_index(drop=True)
        # row data to export
        row_data = data
        # Arrange the data whrer sell and buy will be in the same row, easier to compute (sum of columns)
        data = self.sell_buy_in_same_row_df(data)
        # Weekend
        if self.weekend:
            data = data.loc[data.timestamp_close.dt.strftime("%U")!= data.timestamp_open.dt.strftime("%U")]
        # Add profit columns
        data = self.final_result(data)
        return data,row_data
    