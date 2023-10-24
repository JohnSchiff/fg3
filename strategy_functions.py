from utils import *
import streamlit as st
import s3fs
import matplotlib.pyplot as plt
import io

s3 = s3fs.S3FileSystem()

def remov_row(df):

    # Protection - remove rows where Buy isn't followed by Sell or vice versa
    cond_deals = ((df['open'] == 0) & (df['mispar_hoze'] == df['mispar_hoze'].shift(-1)) |
                  (df['open'] == 1) & (df['mispar_hoze']
                                       == df['mispar_hoze'].shift(1))
                  & ((df['Type'] != df['Type'].shift(-1)) | (df['Type'] != df['Type'].shift(1))))

    # Apply the protection
    df = df[cond_deals].sort_values(by=['mispar_hoze', 'timestamp'])


def get_month_of_file(file):
    month = int(file.split('.parquet')[0][-10:].split('-')[1])
    return month


def set_times(df,min_after_open=5, min_before_close=5):
    """
    This function return the time to get into deal in hte close and in the open 
    """
    
    time_open = df.timestamp.min() + pd.to_timedelta(min_after_open, unit='min')
    time_close = df.timestamp.max() - pd.to_timedelta(min_before_close, unit='min')
    
    return time_open,time_close

def     get_morning_quotes(df):
    df = df.groupby('mispar_hoze').head(1)
    df['open'] = 1

    return df

def get_night_quotes(df):
    df = df.groupby('mispar_hoze').tail(1)
    df['open'] = 0
    return df



def open_close_quotes(df, time_open='10:00', time_close='17:30'):
    '''
    input - dataframe
    output - get first and last quotes by conditions
    '''
    try:
        time_close = pd.to_datetime(time_close).time()
    except:
        pass
    if time_close > df.index.max().time():
        time_close = df.index.max().time()
    time_before_close = (datetime.combine(
        df.index.date[0], time_close) - timedelta(minutes=1)).time()
    try:

        time_open = pd.to_datetime(time_open).time()
    except:
        pass
    time_after_open = (datetime.combine(
        df.index.date[0], time_open) + timedelta(minutes=1)).time()
    cond_times = (df.index.time <= time_close) & (df.index.time >= time_open)
    df = df.loc[cond_times]
    df_close = df.loc[(df.index.time >= time_before_close) & (
        df.index.time <= time_close)].groupby('mispar_hoze').head(1).reset_index()

    df_open = df.loc[(df.index.time >= time_open) & (
        df.index.time <= time_after_open)].groupby('mispar_hoze').tail(1).reset_index()

    return df_open, df_close


def time_close_verify(df, time_close):
    '''
    Protection fir time close not too above 
    '''
    time_close = pd.to_datetime(time_close, format="%H:%M").time()
    latest_time_in_df = df['timestamp'].dt.time.max()
    if latest_time_in_df < time_close:
        time_close = latest_time_in_df

    return time_close


def filter_relevant_options(df, ta35_close):
    
    date = get_date_string(df)
    relevant_options_id = get_relevant_options(ta35_close,date)

    df = df.loc[df.mispar_hoze.isin(relevant_options_id)]
    
    return df
    
    
def filter_open_time(df, time_open):
    time_open_plus_minute = (datetime.combine(datetime.today(), time_open) + timedelta(minutes=1)).time()
    cond_time_open = (df.timestamp.dt.time >= time_open) & (
        df.timestamp.dt.time <= time_open_plus_minute)
    df = df.loc[cond_time_open]
    
    return df

def filter_close_time(df, time_close):
    
    time_close = time_close_verify(df, time_close)
    df = df.loc[df['timestamp'].dt.time < time_close]
    
    return df
    
def filter_by_times(df, time_open, time_close):

    # Make sure all data between  given trading hours time_open to time_close
    # alos good for filtering out to speed up 
    
    time_open = datetime.strptime(time_open, '%H:%M').time()
    time_close = datetime.strptime(time_close, '%H:%M').time()
    cond_time = (df.timestamp.dt.time <= time_close) & (
        df.timestamp.dt.time >= time_open)
    df = df.loc[cond_time]
    
    return df

def filter_by_dte(df, dte_min=1, dte_max=31):
    cond_dte = (df.dte <= dte_max) & (df.dte >= dte_min)
    df = df.loc[cond_dte]

    return df


def filter_by_diff_strike(df, diff_strike, time_close):
    ta35 = get_ta35_per_time(df, time_input=time_close)
    df['diff_strike'] = df['mimush'] - ta35
    cond_diff_strike = (df.diff_strike.isin(diff_strike))
    df = df.loc[cond_diff_strike]

    return df


def filter_by_option_type(df, option_type): 
    cond_option_type = (df.option_type.isin(option_type))
    df = df.loc[cond_option_type]

    return df


def filter_by_weekend(df):
    cond_weekend = (df.week_num_close != df.week_num_open)
    df = df.loc[cond_weekend]

    return df


def get_relevant_quotes(df, time_close='17:30', time_open='10:00', minutes=5, delta=0.5, on='close'):
    if on == 'close':
        time_close = pd.to_datetime(time_close).time()
        df = df.loc[df.index.time <= time_close]
        # latest_time = df.index.max()
        # close_time =latest_time - timedelta(minutes=minutes+delta)
        # end_time = close_time + pd.Timedelta(minutes=delta)
        # df = df.loc[close_time:end_time]

    elif on == 'open':
        # time_open = pd.to_datetime(time_open).time()
        df = df.loc[df.index.time >= time_open]
        earliest_time = df.index.min()
        end_time = earliest_time + pd.Timedelta(minutes=minutes)
        df = df.loc[earliest_time:end_time]
    return df


def get_day_night_table(year_start=2021, year_end=2022, dte_max=31, dte_min=2, diff_from_madad=[0], option_type='C', months=list(range(13)), open_time='10:00', close_time='17:30',
                        app=False):
    months = [months] if type(months) == int else months
    day_night_df = []
    options_id = []

    for year in range(year_start, year_end+1):

        if app is True:
            print(year)
            s3 = s3fs.S3FileSystem()
            files = s3.glob(f's3://schiff-quotes-{year}/{year}/*.parquet')
            q_files = []
            for file in files:
                m = int(file.split('.parquet')[0].split('/')[-1].split('-')[1])
                if m in months:
                    q_files.append(file)
                else:
                    pass

        else:
            q_files = glob.glob(quotes_path+str(year)+'/*')

        for q in q_files:
            if app is True:
                df = pd.read_parquet(q, filesystem=s3)  # read file
            else:
                df = pd.read_parquet(q)
            # set timestanp as index for effiency
            df.set_index('timestamp', inplace=True)
            df = df.loc[(df.dte <= dte_max) &  # filter by days to exipration
                        (df.dte >= dte_min) &
                        (df['type'] == option_type) &
                        (df.index.month.isin(months))]
            df_start = get_relevant_quotes(df, time_open=open_time, on='open')
            df_end = get_relevant_quotes(df, time_close=close_time, on='close')

            diff_mimush_cond = (df_end.diff_mimush >= min(diff_from_madad)) & (
                df_end.diff_mimush <= max(diff_from_madad))
            df_save_end = df_end.loc[diff_mimush_cond].groupby(
                'mispar_hoze').tail(1).reset_index()
            df_save_end['buy_sell'] = 'close'  # identify close and open quotes
            if len(options_id) > 0:
                df_save_start = df_start.loc[df_start.mispar_hoze.isin(
                    options_id)].groupby('mispar_hoze').head(1).reset_index()
                df_save_start['buy_sell'] = 'open'

                day_night_df.append(df_save_start)
            # list of options which traded at close time
            options_id = list(df_save_end.mispar_hoze.values)
            day_night_df.append(df_save_end)

            print(q, 'Added!')

    df = pd.concat(day_night_df)
    df = df.drop(columns=['q1_Bid', 'q2_Bid', 'q1_Ask',
                 'q2_Ask', 'p2_Bid', 'p2_Ask'])

    return df


def get_strategy_results(df, buy_night_sell_morning=True):
    # Day night trades

    night_data = df[df['buy_sell'] == 'close']
    morning_data = df[df['buy_sell'] == 'open']
    morning_data = morning_data.rename(columns={'timestamp': 'time_morning'})
    night_data = night_data.rename(columns={'timestamp': 'time_night'})
    merged_data = pd.merge_asof(night_data, morning_data, left_on='time_night', right_on='time_morning', by=[
                                'mispar_hoze', 'type', 'year', 'month', 'mimush'], direction='forward', suffixes=('_night', '_morning'))
    if buy_night_sell_morning:
        merged_data['pnl'] = merged_data['p1_Bid_morning'] - \
            merged_data['p1_Ask_night']
        # rel_cols =['type','p1_Bid_morning','p1_Ask_night','ta35_night','ta35_morning','pnl','dte_night','month','year']

    else:
        merged_data['pnl'] = merged_data['p1_Bid_night'] - \
            merged_data['p1_Ask_morning']
        # rel_cols =['type','p1_Bid_night','p1_Ask_morning','ta35_night','ta35_morning','pnl','dte_night','month','year']

    result = merged_data.dropna(subset=['time_morning'])
    hodesh = result['time_night'].dt.month
    shana = result['time_night'].dt.year
    result_by_month = result.groupby(hodesh)['pnl'].agg(['sum']).astype(
        int).reset_index().rename(columns={'sum': 'return_per_month', 'time_night': 'month'})
    result_by_year = result.groupby(shana)['pnl'].agg(['sum']).astype(
        int).reset_index().rename(columns={'sum': 'return_per_year', 'time_night': 'year'})
    result_by_year_and_month = result.groupby([shana, hodesh])['pnl'].agg(
        ['sum']).astype(int).rename_axis(('year', 'month')).reset_index()

    return result, result_by_month, result_by_year, result_by_year_and_month, merged_data


## Results ###

def apply_statergy(df, call=True, long=True):

    if call:
        df_type = df.loc[df.type == 'C']
    else:  # Put
        df_type = df.loc[df.type == 'P']

    if long:
        df_type['pnl'] = -df_type['p1_Ask_close'] + df_type['p1_Bid_open']
    else:  # Short
        df_type['pnl'] = df_type['p1_Bid_close'] - df_type['p1_Ask_open']

    df_type['pnl_cumsum'] = df_type['pnl'].cumsum()

    return df_type


def get_drawdown(df):
    '''
    input - dataframe
    output - real drawdown from normal return in percentages 
    '''
    df['pnl_cumsum'] = df.pnl.cumsum()
    t = df['pnl_cumsum'].reset_index(drop=True)  # take column of P&L
    drawdown_nominal = t.min()
    lowest_yield = drawdown_nominal
    highest_yield = t.max()
    for i in t.index:
        current_yield = t.iloc[i]
        min_yield = t.iloc[i:].min()
        drawdown_temp = min_yield - current_yield

        if drawdown_temp < drawdown_nominal:

            drawdown_nominal = drawdown_temp  # change original
            lowest_yield = min_yield
            highest_yield = t.iloc[i]

    st.write(
        f'Drawdown is {drawdown_nominal} ,lowest point is {lowest_yield},Pick point is {highest_yield}')

    return drawdown_nominal


def performance_table(df):
    drawdown = get_drawdown(df)
    n_trades = len(df)
    d = {'Drawdown': drawdown,
         'N': n_trades}
    data = pd.DataFrame(d, index=['Stratergy Results'])

    result_by_month = df.groupby('month')['pnl'].agg(['sum', 'count']).astype(
        int).reset_index().rename(columns={'sum': 'return_per_month', 'count': 'number_of_trades'})
    result_by_diff_strike = df.groupby('diff_mimush_close')['pnl'].agg(
        'sum').astype(int).reset_index().rename(columns={'sum': 'return_per_diff'})
    st.write(data)
    st.write(result_by_month)
    st.write(result_by_diff_strike)


def ta35_plot(df):
    # Find the earliest and latest dates
    earliest_date = df['timestamp_open'].min().date()
    latest_date = df['timestamp_open'].max().date()

    df_ta35 = ta35.loc[ta35['date'].dt.date > earliest_date]
    # Set the figure size directly in subplots()
    fig, ax = plt.subplots(figsize=(12, 6))

    # Rotate the x-axis tick labels for better readability
    ax.tick_params(axis='x', rotation=45)

    ax.plot(df_ta35.date, df_ta35.ta35_index)


def pnl_plot(df):
    fig, ax = plt.subplots()
    ax.plot(df.timestamp_open.dt.date, df.pnl_cumsum)
    # ax.plot(sp500_df.Date, sp500_df.Close, label='S&P 500')
    ax.set_xlabel('Date')
    ax.set_ylabel('Cumulative Sum')
    ax.set_title('Cumulative Sum Over Time')
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, which='both')
    ax.axhline(y=0, color='red')
    # Adjust the size of the plot within the Streamlit app
    fig.set_size_inches(6, 3)  # Adjust the width and height as desired

    # Save the figure to a byte buffer
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', bbox_inches='tight', pad_inches=0.1)
    buffer.seek(0)

    # Display the image in Streamlit with adjusted width
    st.image(buffer, width=600)  # Adjust the width as desired
    ta35_plot(df)

    plt.show()


def pnl_ta35_plot(df):
    fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(12, 9), sharex=True)

    # Plot pnl_cumsum on ax1
    ax1.plot(df.timestamp_open.dt.date, df.pnl_cumsum)
    ax1.set_ylabel('Cumulative Sum')
    ax1.set_title('Cumulative Sum Over Time')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, which='both')
    ax1.axhline(y=0, color='red')

    # Plot ta35_index on ax2
    earliest_date = df['timestamp_open'].min().date()
    latest_date = df['timestamp_open'].max().date()
    df_ta35 = ta35.loc[(ta35['date'].dt.date > earliest_date)
                       & (ta35['date'].dt.date < latest_date)]
    ax2.plot(df_ta35.date, df_ta35.ta35_index)
    ax2.grid(True, which='both')
    ax2.set_ylabel('TA35 Index')
    ax2.tick_params(axis='x', rotation=45)

    # Adjust the size of the plot within the Streamlit app
    fig.set_size_inches(12, 9)  # Adjust the width and height as desired

    # Save the figure to a byte buffer
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', bbox_inches='tight', pad_inches=0.1)
    buffer.seek(0)

    # Display the image in Streamlit with adjusted width
    st.image(buffer, width=800)  # Adjust the width as desired

    plt.show()


def get_ta35_per_time(df, time_input):

    # Convert time input to datetime
    time_input = datetime.strptime(time_input, '%H:%M').time()

    # Prtoection if time input above
    max_time = df.timestamp.dt.time.max()

    # day_in_week = df.timestamp.dt.dayofweek.map(day_of_week_map).iloc[0]
    # if day_in_week == 1:
    #     max_time = time(16, 0, 0)

    if max_time < time_input:
        time_input = max_time
    # filter df to same time
    df_filtered = df.loc[df['timestamp'].dt.strftime(
        '%H:%M') == time_input.strftime('%H:%M')]

    ta35_on_time = df_filtered['ta35'].mode().iloc[0]
    print(f'ta35_on_time is : {ta35_on_time}')
    return ta35_on_time    