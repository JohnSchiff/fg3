import streamlit as st
import pandas as pd
from strategy_classes import Open_close_strategy
import io

# buffer to use for excel writer
# buffer = io.BytesIO()

# from data_processor import process_data
# from strategy import backtest_strategy
# from utils import plot_results
# from constants import DEFAULT_DATA_PATH

# Streamlit UI

# Set page configuration
st.set_page_config(
    page_title="Strategy Backtesting App",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)
st.title("Strategy Backtesting App")



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
    st.error("Error: Start Year is Greater Than End Year",icon="🚨")
months = col3.multiselect(
    'Select Months',
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], default=1)
all_options = col4.checkbox("All months - Full Year", False)
if all_options:
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    st.write('<p style="font-size: 20px; font-weight: bold;">Whole year selected!</p>',
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
    st.error('left side Can not be bigger than right side ')

# Generate df_results
if st.button("Load Data"):
    a = Open_close_strategy(months=[1])
    df = a.proccess_data()

    result = a.final_result(df)
    st.write(result.head())
    
    # Create a BytesIO buffer
    buffer = io.BytesIO()

# Write the DataFrame to the buffer as an Excel file
    result.to_excel(buffer, index=False)
    buffer.seek(0)  # Reset the buffer position to the beginning

    # Add a download button
    st.download_button(
        label="Download Excel File",
        data=buffer,
        key="download_button",
        file_name="my_data.xlsx",
)
    # df_results = process_files(files)
    # st.session_state.df_results = df_results

    # st.write("Results:", df_results)

# Constants
st.sidebar.header("Constants")
st.sidebar.write("Some constant values:")
st.sidebar.header("About")
st.sidebar.write("This is a Streamlit app for backtesting strategies on data.")




# st.sidebar.write("SOME_CONSTANT_1:", SOME_CONSTANT_1)
# st.sidebar.write("SOME_CONSTANT_2:", SOME_CONSTANT_2)

# About
