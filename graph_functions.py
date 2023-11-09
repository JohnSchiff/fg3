import pandas as pd
import numpy as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    if max_time < time_input:
        time_input = max_time
    # filter df to same time
    df_filtered = df.loc[df['timestamp'].dt.strftime(
        '%H:%M') == time_input.strftime('%H:%M')]

    ta35_on_time = df_filtered['ta35'].mode().iloc[0]
    print(f'ta35_on_time is : {ta35_on_time}')
    return ta35_on_time 

def Graph_performance(df,path,low_diff,days_letf_min,days_letf_max):

    TA35_close = pd.read_csv(path+'TA35_yahoo_high_low.csv')
    Date(TA35_close,date_column='Date')
    # Fix datetime
    Date(df,date_column='date')


    fig, (ax1, ax2, ax3) = plt.subplots(3, figsize=(30,10))
    fig.subplots_adjust(hspace=0.5)
    # ax1.set_xlabel("Date", fontsize=10)
    ax1.set_ylabel("Nominal Return",fontsize=10)
    ax1.set_title("Nominal Return  diffrence from madad is "+
    str (low_diff)+' points, days to Exipration from '+str(days_letf_min)+' to '+str(days_letf_max),
    fontsize=12)
    ax2.set_ylabel("Madad TA35",fontsize=10)

    ax3.set_ylabel("Return in Percentages",fontsize=10)
    ax3.set_title(" Return Normalized in pct diffrence from madad is "+
    str (low_diff)+' points,  days to Exipration from '+str(days_letf_min)+' to '+str(days_letf_max),
    fontsize=12)


    ax1.plot(df.date,df.nominal_sum,color='g')
    ax2.plot(TA35_close.Date,TA35_close.Close,color='r',label='Madad Tel Aviv 35')
    ax3.plot(df.date,df.normal_return,color='b')

    ax1.grid(True)
    ax2.grid(True)
    ax3.grid(True)

    ax2.legend()
    
    # format is very important , if not it doesn't save
    plt.savefig(path+'Graphs/'+'days_left_from '+str(days_letf_min)+' to '+
    str(days_letf_max)+' Diff from madad is ' +str(low_diff)+'points'+'.png',format='png') 


    return('Graph was build and exported in' + path)

######################
def graph_nominal_return(df,path):
        Date(df)
        plt.figure(figsize=(10,5))
        plt.grid(True)
        plt.xlabel("Date", fontsize=10)
        plt.ylabel("Return",fontsize=10)
        plt.title("Nominal Return over years", fontsize=7)
        plt.plot(df['date'],df['nominal_sum'])
        locator = mdates.YearLocator()
        # plt.gca().xaxis.set_major_locator(locator)
        # plt.gcf().autofmt_xdate()
        plt.savefig(path+'nominal_Return.png')

def graph_normalized_return(df,path):
        Date(df)
        plt.figure(figsize=(10,5))
        plt.grid()
        plt.xlabel("Date", fontsize=10)
        plt.ylabel("Return",fontsize=10)
        plt.title("Normalized Return over years", fontsize=7)
        plt.plot(df.date,df.normal_return)
        locator = mdates.YearLocator()
        plt.gca().xaxis.set_major_locator(locator)
        plt.gcf().autofmt_xdate()
        plt.savefig(path+'noramlized_Return.png')

