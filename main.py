import streamlit as st
import pandas as pd
from strategy import *
import io

yes_no_mapping = {
    "No": False,
    "Yes": True
}

call_put_mapping = {
    'CALL' : 1,
    'PUT'  : 0    
}
# Streamlit UI
width = 0.5
s_width = width -1

error_message = None


# Set page configuration
st.set_page_config(
    page_title="Strategy Backtesting App",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    )

st.title("Strategy Backtesting App")

option_type_col, time_open_col, time_close_col, diff_min_col, diff_max_col = st.columns([width] * 5)
option_type = option_type_col.selectbox(
    "Option Type",
    ["CALL", "PUT"])
option_type = call_put_mapping[option_type]
# Hour in Open for Buy/Sell
time_open = time_open_col.time_input("Select a time", value=time(10, 00)).strftime('%H:%M')
# Hour in Close for Buy/Sell
time_close = time_close_col.time_input("Select a time", value=time(17, 30)).strftime('%H:%M')

# Diff points is defined:  (strike of option - TA35 index)
diff_min = diff_min_col.number_input("diff strike under", min_value=-50,
                             max_value=0, value=0,  step=10, key='diff_min_input')
diff_max = diff_max_col.number_input("diff strike above ", min_value=0,
                             max_value=50, value=0,  step=10, key='diff_max_input')

diff_strike = list(range(diff_min, diff_max+10,10))

start_year_col, end_year_col, months_input_col, all_months_col, startegy_type_col = st.columns([width]*2 +[0.2, 0.2,0.2])  # size of columns

# Years
start_year = start_year_col.number_input("start year ", min_value=2015,
                               max_value=2021, value=2021, step=1, key="start_year_input")
end_year = end_year_col.number_input("end year", min_value=2015,
                             max_value=2021, value=2021,  step=1, key="end_year_input")
years = list(range(start_year, end_year+1))

# Protection for order
if start_year > end_year:
    error_message = "Error: Start Year is Greater Than End Year"
    st.error(error_message, icon="🚨")
    
months_input = months_input_col.multiselect(
    'Select Months',
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], default=1)

all_months = all_months_col.checkbox("Full Year", False)
if all_months:
    months_input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    st.write('<p style="font-size: 20px; font-weight: bold;">Whole year selected!</p>',
             unsafe_allow_html=True)
startegy_type = startegy_type_col.selectbox("Strategy",[ "Long", "Short"])
    
col1, col2, col3 = st.columns([0.1] * 3 )  # size of columns
# Days to expiration
dte_min = col1.number_input(
    "From Days to exipration", min_value=2, max_value=31, value=2, step=1, key="dte_min_input")
dte_max = col2.number_input(
    "To Days to exipration ", min_value=2, max_value=31, value=31,  step=1, key="dte_max_input")

weekend = col3.selectbox("Weekend Only",["No", "Yes"])
weekend = yes_no_mapping[weekend]

# Protection for order
if dte_min > dte_max:
    error_message = "Left must be lower or equal to Right"
    st.error(error_message, icon="🚨")
    
col1, col2, col3 = st.columns([0.2] * 3 )  # size of colum
# Generate df_results
if col1.button("Day_Night") and error_message is None:
    s1 = Open_close_strategy(time_close=time_close,
                            time_open=time_open,months=months_input,
                            years=years,weekend= weekend,
                            dte_max =dte_max, dte_min=dte_min,diff_strike=diff_strike,
                            option_type = option_type, strategy_type=startegy_type)
    df,row_data = s1.proccess_data()
    st.write(df)
    col1, col2, col3 = st.columns([width] * 3 )  # size of columns

    col1.write('Results Per Year')
    results_by_year = s1.group_and_sum(df, group_by='year')
    col1.write(results_by_year)
    col2.write('Results Per Month')
    results_by_month = s1.group_and_sum(df, group_by='month')
    col2.write(results_by_month)    
    col3.write('Results Per Diffrenec Strike from index')
    results_by_strike = s1.group_and_sum(df, group_by='diff_strike')
    col3.write(results_by_strike)
    
    # Create a BytesIO buffer
    buffer = io.BytesIO()

# Write the DataFrame to the buffer as an Excel file
    row_data.to_excel(buffer, index=False)
    buffer.seek(0)  # Reset the buffer position to the beginning
# Add a section for the download button
    st.subheader('Download Data')
    # Add a download button
    st.download_button(
        label="Download Excel File",
        data=buffer,
        key="download_button",
        file_name="data.xlsx",
                 )

# Constants
st.sidebar.header("About")
st.sidebar.write("This is a Streamlit app for backtesting strategies on data.")




