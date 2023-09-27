# import modules
import glob
from datetime import datetime
import matplotlib.pyplot as plt
import patoolib
import pandas as pd
import numpy as np
import matplotlib.dates as mdates
import shutil
import os
import sys
pd.set_option('display.max_rows', 201)
pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None  # Disable the warning



# project main path
path = 'C:\\Users\\yschiff\\OneDrive - Cisco\\Desktop\\FG3/'# os.getcwd()+'/'


# data path
years_path = path + "data/"

# folder to extract Trades files
extract_path_Trades = path + "extract_files_trades/"

# folder to extract Quotes files

extract_path_Quotes = path+'extract_files_quotes/'

quotes_path = path+'quotes/'

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

    return print('Folder Created')


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



# ============================
# ============================
# ===  Trades Functions ===
# ============================
# ============================

#############################


def filter_Trade_File(file):
    '''
    input - trade files  csv format
    output -  return dataframe from file with relevant columns
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

    return (df)


def id_options(df):

    # dict for Months relevant for early
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

    return (id)


def madad_basis(df, ta35):
    '''
    Add real time index from TA35.csv which has the madad basis
    '''

    # Add 2 zero because time is 6  digits
    ta35['time'] = ta35.time*100
    # Merge by the closest time
    df = pd.merge_asof(df.sort_values('time'),
                       ta35.sort_values('time'), on='time',
                       by=['date'])
    # rounded madad
    df['Adj_madad'] = round(df.madad_basis, -1)
    # sort dataframe
    df = df.sort_values(by=['mispar_hoze', 'date', 'time'])

    # points Above/Below current madad
    df['diff_mimush'] = df.mimush - df.Adj_madad

    print('Real Madad basis columns Added')
    return (df)


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


def quotes_merge_with_option_details(df, df_options_id):\

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

    # Correct the ta35 values by mapping values and forward-fill NaN values
    df['ta35'] = df['timestamp'].dt.strftime('%H:%M:%S').map(
        mapping_dict).fillna(method='ffill').astype(int)

    # Crete column of diffrent strike from ta35
    df['diff_strike'] = df['mimush'] - df['ta35']

    # Foramtting int16 for effciency
    df['diff_strike'] = df['diff_strike'].astype('int16')
    df['ta35'] = df['ta35'].astype('int16')

    # Drop columns
    df.drop(columns=['name', 'pkiya'], inplace=True)

    return df
