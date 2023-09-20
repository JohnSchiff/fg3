
from build_up_functions import *
print(path)
ta35 = pd.read_csv(path+'ta35/TA_35_Historical_Data.csv', parse_dates=['Date'])
ta35 = ta35.rename(columns={'Date': 'date'})
# for year in range(2022,2023):
#     ta35_yearly = []
#     [quotes_files] =  glob.glob(quotes_path+str(year)+'/*',recursive=True)

#     for file in quotes_files:
#         df = pd.read_parquet(file)

#         # remove time components that make problems with time delta
#         current_day = pd.to_datetime(df.timestamp).dt.floor('D')
#         date = current_day.iloc[0]

#         # Get option details by its id ('mispar_hoze' column)
#         if 'dte' not in df.columns:
#             df = df.merge(option_details,on='mispar_hoze').drop('name',axis=1)
#             # make it datetime object to substract
#             expiration_day = pd.to_datetime(df.pkiya)
#         # create column days left to exipration
#             df['days_to_exp'] = (expiration_day-current_day).dt.days

#         # get High of the date
#         high_dayly = ta35.loc[ta35.date==date].High.iloc[0]
#         low_dayly = ta35.loc[ta35.date == date].Low.iloc[0]

#         # dataframe with 2 columns , timestamp and ta35 at the moment
#         ta35_dayly = df.query('ta35>=@low_dayly and ta35<=@high_dayly').groupby('timestamp').apply(
#             lambda x: x.ta35.mode()[0]).reset_index().rename(columns={0: 'ta35_index'})
#         ta35_dayly['ta35_index'] = ta35_dayly.groupby(ta35_dayly['timestamp'].dt.minute,group_keys=False)['ta35_index'].transform(lambda x: x.mode()[0])

#         ta35_yearly.append(ta35_dayly)

#         print(f'{file} appended!')


#     result = pd.concat(ta35_yearly)
#     result.to_parquet('ta35/TA35_'+str(year)+'.parquet', index=False)


# loop over chosen years
for year in range(2020, 2021):

    # open new folder to store files of quotes which will be extracted
    create_folder(extract_path_Quotes)

    # get list of all Quotes files "/B" by year  which are zipped
    zip_Quotes_files = glob.glob(
        years_path+str(year)+'/M*/D*/B/*', recursive=True)
    # Check if there is Unzip files
    Check_unzip_files(zip_Quotes_files, extract_path=extract_path_Quotes)

    unzip_Quotes_files = Unzip_files(
        zip_Quotes_files[0:1], extract_path_Quotes)
    id_options_year_path = path + "id_options/id_options" + str(year)+'.csv'
    # dataframe of option's details
    df_options_id = pd.read_csv(id_options_year_path)
    for quotes_file in unzip_Quotes_files:

        # reset index to have make it parsable
        df = pd.read_csv(quotes_file).reset_index()
        delete_file(quotes_file)
        df_quotes = quotes_parser(df)
        df_quotes = quotes_filter(df_quotes)
        df_quotes = quotes_merge_with_option_details(df_quotes, df_options_id)
        df_quotes = quotes_add_computed_ta35_index(df_quotes, ta35)
