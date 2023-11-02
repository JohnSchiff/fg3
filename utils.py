# import modules
import glob
from datetime import datetime, timedelta, time
import matplotlib.pyplot as plt
import patoolib
import pandas as pd
import numpy as np
import shutil
import os
import sys
pd.set_option('display.max_rows', 201)
pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None  # Disable the warning



# project main path
path = 'C:\\Users\\yschiff\\OneDrive - Cisco\\Desktop\\FG3/'# os.getcwd()+'/'


# data path
data_path = path + "data/"

# folder to extract Trades files
extract_path_Trades = path + "extract_files_trades/"

extract_folder = path + "extract_folder/"
# folder to extract Quotes files

extract_path_Quotes = path+'extract_files_quotes/'

quotes_path = path+'quotes/'
day_of_week_map = {0: 2, 1: 3, 2: 4,
                   3: 5, 4: 6, 5: 7, 6: 1}

ta35 = pd.read_csv(path+'ta35/TA_35_Historical_Data.csv', parse_dates=['Date'])
ta35 = ta35.rename(columns={'Date': 'date'})

# ============================
# ============================
# ===  Global FUNCTIONS  ===
# ============================
# ============================


def create_folder(path_of_dir):
    '''
    create folder or (if exist) recreate folder and delete previous one
    input - path     
    '''
    if os.path.exists(path_of_dir):
        shutil.rmtree(path_of_dir)  # Deleting folder

    os.mkdir(path_of_dir)

    return print(f'Folder created in {path_of_dir}')


def delete_file(file):
    '''
    delete single file if exist if not return "Deosnt exist" 
    '''
    # checking if file exist or not
    if (os.path.isfile(file)):
        # os.remove() function to remove the file
        os.remove(file)
        # Printing the confirmation message of deletion
        #print("File Deleted successfully")
    else:
        return ("File does not exist")

    return (file, 'Deleted')


def Copy_files(files_list, dir_to_paste):
    '''
    copy list of files to one directory
    '''
    # move regular files which is not zipped to Extracted folder
    n = 0
    for file in files_list:
        shutil.copy(file, dir_to_paste+str(n)+'Copied.txt')
        n = n+1

    return ('all files coppied')


def Check_unzip_files(files, extract_path):
    '''
    All files should be zipped ,this function check if fro some reason there
    are Unzip files.
    if there are, Add them  to the list of Unzip files 
    if not -  Return Empty list and print "no unzip files founded"

    input - all files in folder of some year
    output - list of Unzip files in a folder OR print
    '''
    unzip_files = []
    for file in files:
        try:
            pd.read_csv(file, nrows=1)
            unzip_files.append(file)

        except:
            pass

    # if unzip files founded
    if len(unzip_files) > 0:

        print("Unzip files in folder founded....  copying to ")

        Copy_files(files_list=unzip_files, dir_to_paste=extract_path)

        return ('copied')
    # if no unzip files found
    else:
        return print("No Unzip files founded in folder")


def Unzip_files(zip_files, extract_path):
    for zip_file in zip_files:
        print(zip_file, '\n')
        patoolib.extract_archive(zip_file, outdir=extract_path, verbosity=0)

    unzip_files = glob.glob(extract_path+'/*', recursive=True)
    return unzip_files


def extract_single_file(file, extract_path):
    '''
    input - Zipped file (file) 

    output - Extract zipped file to folder "Extract files"
    '''
    print(file, '\n')

    # out will be the extract file
    out = extract_path

    try:
        patoolib.extract_archive(file, outdir=out, verbosity=0)
    except Exception as e:
        print('An error occurred while extracting the archive:')
        print(str(e))
        print('The type of the exception was:')
        print(type(e))
        print('The traceback information was:')
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        traceback.print_tb(exc_traceback)

        # print if succeed
    return ('file Successfully extracted by name in ', out)


def extract_rar_files(rar_files, extract_path):
    '''
    input - Zipped files (rar_files) 

    output - Extract zipped files to folder "Extract files"

    '''
    # if zipped files is more than one, etc LIST
    if isinstance(rar_files, list):

        i = 1
        for file in rar_files:
            print(file, '\n')

            # out will be the extract file
            out = extract_path+'/'+str(i)

            try:
                patoolib.extract_archive(
                    file, outdir=out, verbosity=0)  # Extract zip file
            except:
                print('couldnd__extract')  # if Problem

            i = i+1

            # print if succeed
            print('folder create by name', out)

    else:  # if only one zip file in folder
        extract_single_file(rar_files, extract_path)


def get_date_string(df):
    '''
    Return date of quotes file from dataframe based on 'timestamp' column
    
    '''
    date = str(df['timestamp'].dt.date.iloc[0])
    
    return date


def add_options_details(df, year):
    
    id_optios_file = glob.glob(path+f'id_options/*{year}*')[0]
    rel_cols = ['mispar_hoze', 'option_type', 'mimush', 'month', 'year','pkiya']
    df_id = pd.read_csv(id_optios_file, usecols=rel_cols)
    df = df.merge(df_id, on='mispar_hoze', how='left')
    
    return df


def get_quotes_by_years(years):
    if not isinstance(years, list):
    # If 'years' is not a list, convert it to a list with one element
        years = [years]
    files = []
    for year in years:
        quotes_files = glob.glob(quotes_path+f'/{year}/*')
        files.extend(quotes_files)
    return files



def ta35_df_by_date(df):
    '''
     Get ta35 index file of specific date
     
    '''
    date = get_date_string(df)

    year = date.split('-')[0]
    ta35_files = glob.glob(path+'ta35/'+str(year)+'/*')
    for f in ta35_files:
        if f.split('.parquet')[0][-10:] == date:
            df_ta35 = pd.read_parquet(f)
            df_ta35.timestamp = pd.to_datetime(
                df_ta35.timestamp, format="%H:%M:%S")
            return df_ta35
    return "No date was found"    


def get_ta35_by_time(df, time_input):
    # time_input = pd.to_datetime(time_input, format="%H:%M")
    df_ta35 = ta35_df_by_date(df)
    # Check if time input is in timestamp column
    if not (df_ta35['timestamp'] == time_input).any(): 
        time_differences = abs(df_ta35['timestamp'] - time_input)
        # Get the closest time 
        time_input = df_ta35.loc[time_differences.idxmin(), 'timestamp']
        
    ta35_value = df_ta35.loc[df_ta35['timestamp'] == time_input, 'ta35'].iloc[0]

    return ta35_value


def remove_non_trade_rows(df):

    df = df[['mispar_hoze','p1_Bid','p1_Ask','timestamp','open']]
    df.sort_values(by=['mispar_hoze', 'timestamp'], inplace=True)

    # Filter close rows which has same id After, open is 0 and open after that is 1
    close_rows = ((df['mispar_hoze'] == df['mispar_hoze'].shift(-1))
                & (df.open == 0) & (df.open.shift(-1) == 1))

    # Filter open rows which has same id Before, open is 1 and open after that is 0

    open_rows = ((df['mispar_hoze'] == df['mispar_hoze'].shift(1))
                & (df.open == 1) & (df.open.shift(1) == 0))

    df = df.loc[close_rows | open_rows]


    return df


def filter_option_type(df, call=True):
    option_is = 0
    if call:
        option_is = 1
    df = df.loc[df.option_type == option_is]

    return df
  
    
def sell_buy_in_same_row_df(df, short=False):

    # asssign trade id for each trade
    df['trade_id'] = (df.index // 2) + 1

    if short:
        sell = 'p1_Bid'
        buy = 'p1_Ask'
        cols = ['trade_id', 'mispar_hoze', 'sell',
                'buy', 'timestamp_close', 'timestamp_open']
    else:
        sell = 'p1_Ask'
        buy  = 'p1_Bid'
        cols = ['trade_id', 'mispar_hoze', 'buy',
                'sell', 'timestamp_close', 'timestamp_open']
    # Define custom aggregation functions
    agg_funcs = {
        'mispar_hoze': 'first',
        sell: 'first',
        buy: 'last',
        'timestamp': ['first', 'last']  # Aggregate both first and last timestamps

    }

    # Group by 'trade_id' and apply custom aggregation functions
    result_df = df.groupby('trade_id', as_index=False).agg(agg_funcs)

    # Rename the resulting columns for clarity
    result_df.columns = cols
    
    result_df.sort_values(by='timestamp_close',inplace=True)
    
    return result_df

# ============================
# ============================
# ===  Trades Functions ===
# ============================
# ============================

#############################


def filter_Trade_File(file):
    '''
    input - trade files  csv format
    output - return dataframe from file with relevant columns
            trades which: 
            1.not cancelled and 
            2.only TA35 MAOF  
           filtering with conditions (cond variable)
    '''
    # columns of file
    col_iskaot = ["date", "kod", "mispar_hoze", "name", "time", "mispar_iska", "cancel",
                  "p", "q", "hishtatfut", "mispar_pkuda_B", "mispar_pkuda_S", "match_trades"]

    # relevant columns
    relevant_cols_iskaot = ['date', 'mispar_hoze',
                            'name', 'kod', 'time', 'p', 'q', 'cancel']

    # import csv
    df = pd.read_csv(file, sep=',', header=0, names=col_iskaot,
                     usecols=relevant_cols_iskaot)

    # conditinons kod=1 >> only TA35
    # cancel=0 >> Trades which not cancelled.
    cond = (df.kod == 1) & (df.cancel == 0)

    # filter dataframe based on conditions above
    df = df.loc[cond].drop(columns={'kod', 'cancel'}).drop_duplicates()

    df = date_to_timestamp(df)

    return df


def id_options(df):

    # dict for Months relevant before 2015
    months = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
              'MAY': '05', 'JUN': '06', 'JUL': '07',
              'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

    # dict for Years
    years = {'4': '2014', '5': '2015', '6': '2016', '7': '2017',
             '8': '2018', '9': '2019',
             '0': '2020', '1': '2021',
             '2': '2022', '3': '2023'}

    id = df.loc[:, ['mispar_hoze', 'name']].drop_duplicates()

    monthly_option_pattern = r"[PC]\d{6}([M])"
    id = id[id['name'].str.contains(
        monthly_option_pattern)].reset_index(drop=True)

    type_option_pattern = '-(C|P)'
    id['option_type'] = id['name'].str.extract(type_option_pattern)[0]
    id['option_type'].replace({'P': 0, 'C': 1}, inplace=True)

    strike_pattern = r'(\d{4})M'
    id['mimush'] = id['name'].str.extract(strike_pattern)[0].astype('int32')

    month_pattern = r"(\d{2})$"
    id['month'] = id['name'].str.extract(month_pattern).astype('int32')

    year_pattern = r'M(\d{1})'
    id['year'] = id['name'].str.extract(
        year_pattern)[0].map(years).astype('int32')

    return id





# ============================
# ============================
# ===  FUNCTIONS For Quotes ===
# ============================
# ============================


def quotes_parser(df):

    col_supply = ["sug", "date", "time", "kod", "mispar_hoze", "p1_Ask", "p2_Ask",
                  "p3", "p4", "p5", "q1_Ask", "q2_Ask", "q3", "q4", "q5",
                  "ta35", 'n1', 'n2', 'n3']

    relevant_col_supply = ['sug', 'date', 'time', 'mispar_hoze',
                           'p1_Ask', 'p2_Ask', 'q1_Ask', 'q2_Ask', 'ta35']

    col_demand = ["sug", "date", "time", "kod", "mispar_hoze", "p1_Bid",
                  "p2_Bid", "p3", "p4", "p5", "q1_Bid", "q2_Bid", "q3", "q4", "q5",
                  "last_shaar", "sug_shaar", "mahzor_yomi", "kod_iska_toemet"]
    relevant_cols_demand = ['sug', 'date', 'time', 'mispar_hoze', 'p1_Bid',
                            'p2_Bid', 'q1_Bid', 'q2_Bid']

    ask_columns = ['sug', 'date', 'time', 'mispar_hoze',
                   'p1_Ask', 'p2_Ask', 'q1_Ask', 'q2_Ask', 'ta35']

    bid_columns = ['sug', 'date', 'time', 'mispar_hoze', 'p1_Bid',
                   'p2_Bid', 'q1_Bid', 'q2_Bid']

    # make dictionary of the columns on the ASK side
    ask_columns = {df.columns[i]: col_supply[i]
                   for i, column in enumerate(df.columns)}

    # change columns from level 0,1, etc to real columns of ASK side
    ask = df.rename(ask_columns, axis=1)

    # keep only Ask (sug=3) and TA35 Options (kod=1)  Keep relevant ask columns
    ask = ask.loc[(ask.sug == 3) & (ask.kod == 1), relevant_col_supply]

    # make dictionary of the columns on the BID side
    bid_columns = {df.columns[i]: col_demand[i]
                   for i, column in enumerate(df.columns)}

    # change columns from level 0,1, etc to real columns of BID side
    bid = df.rename(bid_columns, axis=1)

    # keep only BID (sug=2) and TA35 Options (kod=1)     # Keep relevant bid columns

    bid = bid.loc[(bid.sug == 2) & (bid.kod == 1), relevant_cols_demand]

    # Merge BID dataframe and ASK Dataframe into one
    full_quotes = pd.merge(bid, ask, on=['date', 'time', 'mispar_hoze']).drop(
        columns=['sug_x', 'sug_y'])  # drop non relevant columns

    # filter NaN rows
    full_quotes.dropna(inplace=True)

    return full_quotes


def quotes_filter(df_quotes):

    # remove rows where there is no price/quantity
    df_quotes = df_quotes.query(
        '`p1_Bid` > 0 and `p2_Bid` > 0 and  `q1_Bid` > 0 \
            and `q2_Bid` and `p1_Ask` > 0 and `p2_Ask`>0 \
                and `q1_Ask` > 0 and  `q2_Ask` > 0 ')
    
    # columns to change from float to int32
    int_32_cols = ['p1_Bid', 'p2_Bid', 'p1_Ask', 'p2_Ask']
    int_16_cols = ['q1_Bid', 'q2_Bid', 'q1_Ask', 'q2_Ask']

    # chage type of columns from float to integers
    df_quotes[int_32_cols] = df_quotes[int_32_cols].astype('int32')
    df_quotes[int_16_cols] = df_quotes[int_16_cols].astype('int16')
    
    # converrt ta35 float 32
    df_quotes['ta35'] = df_quotes['ta35'].astype('float32')
    df_quotes = date_to_timestamp(df_quotes)

    return df_quotes


def quotes_merge_with_option_details(df, df_options_id):

    df = df.merge(df_options_id, on='mispar_hoze',
                  how='left').dropna(subset='name')

    efficient_data_types = {'mispar_hoze': 'category',
                            'mimush': 'int16',
                            'month': 'int8',
                            'year': 'int16',
                            'option_type': 'int8'

                            }

    df = df.astype(efficient_data_types)

    df['pkiya'] = pd.to_datetime(df['pkiya'], format='%Y-%m-%d')

    df['dte'] = (df.pkiya - pd.to_datetime(df.timestamp.dt.date,
                                           format='%Y-%m-%d')).dt.days.astype('int16')

    return df


def quotes_add_computed_ta35_index(df, ta35):
    # dataframe just relevant columns
    df_filtered = df[['ta35', 'mimush', 'dte', 'timestamp']]

    # Get the cuurent date
    current_day = pd.to_datetime(df_filtered.timestamp).dt.floor('D')
    date = current_day.iloc[0]
    # round the original ta35 column from data
    df_filtered['ta35'] = (df_filtered['ta35'].round(-1)).astype(int)

    # Get High of day rounded
    high_dayly = ta35.loc[ta35.date == date].High.iloc[0]
    high_dayly = round(high_dayly, -1)
    # Get Low of day rounded
    low_dayly = ta35.loc[ta35.date == date].Low.iloc[0]
    low_dayly = round(low_dayly, -1)

    # filter df  that ta35 in the range of high and low and get each timestamp the most common value
    df_rel = pd.DataFrame(df_filtered.loc[(df_filtered.ta35 >= low_dayly) & (df_filtered.ta35 <= high_dayly) &
                                          (df.mimush >= low_dayly) & (df.mimush <= high_dayly)].groupby(
        'timestamp').ta35.value_counts().reset_index())

    # Calculate the most common value for each 'timestamp' group
    most_common_values = df_rel.groupby(df_rel['timestamp'].dt.strftime('%H:%M:%S'))['ta35'].apply(
        lambda x: x.mode().iloc[0]).reset_index()

    # Create a mapping dictionary timestamp and value of ta35
    mapping_dict = dict(
        zip(most_common_values['timestamp'], most_common_values['ta35']))
    
    # Sort by time
    df.sort_values(by='timestamp' ,inplace=True) 
    
    # Correct the ta35 values by mapping values and forward-fill NaN values
    df['ta35'] = df['timestamp'].dt.strftime('%H:%M:%S').map(
        mapping_dict).fillna(method='ffill').fillna(method='bfill').astype(int)

    # Crete column of diffrent strike from ta35
    df['diff_strike'] = df['mimush'] - df['ta35']

    # Foramtting int16 for effciency
    df['diff_strike'] = df['diff_strike'].astype('int16')
    df['ta35'] = df['ta35'].astype('int16')

    # Drop columns
    df.drop(columns=['name','pkiya'], inplace=True)

    return df


def date_to_timestamp(df):
    # convert the integer columns to strings and zero-pad them if necessary
    df['date'] = df['date'].astype(str).str.zfill(8)
    df['time'] = df['time'].astype(str).str.zfill(8)

    # combine the date and time columns into a single string column
    datetime_str = df['date'] + df['time']

    # convert the datetime string to a timestamp column
    df['timestamp'] = pd.to_datetime(datetime_str, format='%Y%m%d%H%M%S%f')

    # remove "date" and "time" columns
    df.drop(columns=['date', 'time'], inplace=True)

    return df
