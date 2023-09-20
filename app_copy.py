import pandas as pd
import s3fs
import strategy_functions
from strategy_functions import *
import streamlit as st
import base64
import tempfile
import matplotlib.pyplot as plt

a = 11
# a = pd.read_csv('ta35_close_for_results.csv', parse_dates=[
#                 'date']).sort_values(by='date')
# b = a.loc[(a.date.dt.month == 1) & (a.date.dt.year == 2021)]


# # Set the figure size directly in subplots()
# fig, ax = plt.subplots(figsize=(12, 6))

# # Rotate the x-axis tick labels for better readability
# ax.tick_params(axis='x', rotation=45)

# ax.plot(b.date, b.ta35_index)
# plt.show()

# ==============================================================================================================
# Backtesting TA35 options
# ==============================================================================================================

# Main title
st.set_page_config(page_title="Backtesting Options",
                   page_icon=":guardsman:", layout="wide")
st.title(" Backtesting TA35 options  ")
relevant_cols = ['timestamp_open', 'timestamp_close', 'ta35_index_open', 'ta35_index_close',
                 'strike', 'p1_Bid_close', 'p1_Ask_close', 'p1_Bid_open', 'p1_Ask_open', 'mispar_hoze']
col1, col2, col3, col4, col5 = st.columns([0.5, 0.5, 0.5, 0.5, 0.5])

option_type = col1.selectbox(
    "Option Type",
    ["CALL", "PUT", "Both"])

# Hour in Open for Buy/Sell
open_time = col2.time_input('open time', value=pd.Timestamp(
    year=2022, month=1, day=1, hour=10, minute=0))

# Hour in Close for Buy/Sell
close_time = col3.time_input('close time', value=pd.Timestamp(
    year=2022, month=1, day=2, hour=17, minute=30))

# Diff points is defined:  (strike of option - TA35 index)
diff_min = col4.number_input("diff strike under", min_value=-50,
                             max_value=50, value=0,  step=10, key='diff_min_input')
diff_max = col5.number_input("diff strike above ", min_value=-50,
                             max_value=50, value=0,  step=10, key='diff_max_input')

col1, col2, col3, col4 = st.columns([0.54, 0.5, 0.5, 0.5])  # size of columns

# Years
start_year = col1.number_input("start year ", min_value=2015,
                               max_value=2021, value=2021, step=1, key="start_year_input")
end_year = col2.number_input("end year", min_value=2015,
                             max_value=2021, value=2021,  step=1, key="end_year_input")

# Protection for order
if start_year > end_year:
    st.warning('start year must be lower than end year')
months = col3.multiselect(
    'Select Months',
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], default=1)

all_options = col4.checkbox("All months - Full Year", False)
if all_options:
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    st.write('<p style="font-size: 20px; font-weight: bold;">All months of year is selected!!</p>',
             unsafe_allow_html=True)

col1, col2, col3 = st.columns([0.5, 0.5, 0.5])  # size of columns

# Days to expiration
dte_min = col1.number_input(
    " From", min_value=2, max_value=31, value=2, step=1, key="dte_min_input")
dte_max = col2.number_input(
    "To ", min_value=2, max_value=31, value=31,  step=1, key="dte_max_input")
weekend = col3.selectbox(
    "Weekend",
    ["No", "Yes"])

# Protection for order
if dte_min > dte_max:
    st.warning('left side Can not be bigger than right side ')


display_columns = ['pnl', 'pnl_cumsum']
short_columns = ['p1_Ask_open', 'p1_Bid_close'] + display_columns
short_columns_mapping = {'p1_Bid_close': 'sell', 'p1_Ask_open': 'buy'}

long_columns_mapping = {'p1_Bid_open': 'sell', 'p1_Ask_close': 'buy'}
long_columns = ['p1_Bid_open', 'p1_Ask_close'] + display_columns
s3 = s3fs.S3FileSystem()
files = s3.glob(f's3://schiff-quotes-2021/2021/*.parquet')
files = search_relevant_months(files, months_list=months)


@st.cache
def process_files(files):
    rel_cols = ['timestamp', 'mispar_hoze', 'p1_Bid', 'p1_Ask', 'madad',
                'type', 'strike', 'month', 'year', 'diff_mimush', 'dte', 'ta35_index']
    l = []

    for file in files:
        print(file)
        df = pd.read_parquet(file, columns=rel_cols, filesystem=s3)
        df.set_index('timestamp', inplace=True)
        df = clean_outliers_quotes(df)
        df = filter_by_option_type(df, option_type=option_type)
        df_open, df_close = open_close_quotes(df, open_time, close_time)
        df_close = filter_by_dte(df_close, dte_min, dte_max)
        df_close = filter_by_diff_strike(
            df_close, diff_min=diff_min, diff_max=diff_max)
        if file == files[0]:
            df_prev_day = df_close
            continue
        id_options_close = df_prev_day.mispar_hoze.to_list()

        df_open = df_open.loc[df_open.mispar_hoze.isin(id_options_close)]
        mrg = df_open.merge(df_prev_day, on=[
                            'mispar_hoze', 'type', 'strike', 'month', 'year'], suffixes=('_open', '_close'))
        mrg['week_num_open'] = mrg['timestamp_open'].dt.isocalendar().week
        mrg['week_num_close'] = mrg['timestamp_close'].dt.isocalendar().week
        if weekend == "Yes":
            mrg = filter_by_weekend(mrg)
        mrg['pnl'] = -mrg.p1_Ask_close + mrg.p1_Bid_open
        l.append(mrg)

        df_prev_day = df_close

    df_results = pd.concat(l).reset_index(drop=True)
    return df_results


# Generate df_results
if st.button("Load Data"):
    st.write(files)
    df_results = process_files(files)
    st.session_state.df_results = df_results

    st.write("Results:", df_results)

# Access df_results
if option_type.upper() == 'CALL':
    if st.button("Long CALL"):
        if 'df_results' not in st.session_state:
            st.write("Please click 'Test' first to generate df_results")
        else:
            df = st.session_state.df_results
            df_long_call = apply_statergy(df, long=True, call=True)
        st.write(df_long_call[long_columns].rename(
            columns=long_columns_mapping))
        pnl_ta35_plot(df_long_call)
        performance_table(df_long_call)
        st.download_button(
            "Download Results",
            df_long_call.to_csv(),
            file_name="df_long_call.csv",
            mime="text/csv")
    if st.button("Short CALL"):
        if 'df_results' not in st.session_state:
            st.write("Please click 'Test' first to generate df_results")
        else:
            df = st.session_state.df_results
            df_short_call = apply_statergy(df, long=False, call=True)

        st.write(df_short_call[short_columns].rename(
            columns=short_columns_mapping))
        pnl_ta35_plot(df_short_call)
        performance_table(df_short_call)

        st.download_button(
            "Download Results",
            df_short_call.to_csv(),
            file_name="df_short_call.csv",
            mime="text/csv")
# Access df_results
if option_type.upper() == 'PUT':

    if st.button("Long PUT"):
        if 'df_results' not in st.session_state:
            st.write("Please click 'Test' first to generate df_results")
        else:
            df = st.session_state.df_results
            df_long_put = apply_statergy(df1, long=True, call=False)

        st.write(df_long_put[long_columns].rename(
            columns=long_columns_mapping))
        pnl_plot(df_long_put)
        performance_table(df_long_put)

        st.download_button(
            "Download Results",
            df_long_put.to_csv(),
            file_name="df_long_put.csv",
            mime="text/csv")

    if st.button("Short PUT"):
        if 'df_results' not in st.session_state:
            st.write("Please click 'Test' first to generate df_results")
        else:
            df = st.session_state.df_results
            df_short_put = apply_statergy(df, long=False, call=False)
        st.write(df_short_put[short_columns].rename(
            columns=short_columns_mapping))
        pnl_plot(df_short_put)
        performance_table(df_short_put)
        st.download_button(
            "Download Results",
            df_short_put.to_csv(),
            file_name="df_short_put.csv",
            mime="text/csv")
