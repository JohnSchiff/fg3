
from utils import *
from strategy_functions import *
ta35 = pd.read_csv(path+'ta35/TA_35_Historical_Data.csv', parse_dates=['Date'])
ta35 = ta35.rename(columns={'Date': 'date'})

# loop over chosen years
for year in range(2022, 2023):

    # open new folder to store files of quotes which will be extracted
    create_folder(extract_path_Quotes)

    # get list of all Quotes files "/B" by year  which are zipped
    zip_Quotes_files = glob.glob(
        years_path+str(year)+'/M*/D*/B/*', recursive=True)
    # Check if there is Unzip files
    Check_unzip_files(zip_Quotes_files, extract_path=extract_path_Quotes)

    unzip_Quotes_files = Unzip_files(
        zip_Quotes_files, extract_path_Quotes)
    id_options_file = path + "id_options/all_options_new.csv"
    # dataframe of option's details
    df_options_id = pd.read_csv(id_options_file)
    df_options_id = df_options_id.dropna(subset='pkiya')
    df_options_id['pkiya'] = pd.to_datetime(df_options_id.pkiya)

    for quotes_file in unzip_Quotes_files:

        # reset index to have make it parsable
        df = pd.read_csv(quotes_file).reset_index()
        delete_file(quotes_file)
        df_quotes = quotes_parser(df)
        df_quotes = quotes_filter(df_quotes)

        current_day = pd.to_datetime(df_quotes.timestamp).dt.floor('D')
        date = current_day.iloc[0]
        # Get High of day rounded
        high_dayly = ta35.loc[ta35.date == date].High.iloc[0]
        high_dayly = round(high_dayly, -1)
        # Get Low of day rounded
        low_dayly = ta35.loc[ta35.date == date].Low.iloc[0]
        low_dayly = round(low_dayly, -1)
        a = df_quotes.merge(df_options_id[['mispar_hoze', 'pkiya', 'mimush']],
                            on='mispar_hoze', how='left').dropna(subset='mimush')
        a['mimush'] = a['mimush'].astype('int16')
        a['dte'] = (a['pkiya'] - date).dt.days
        a = a.loc[(a.mimush >= low_dayly) & (a.mimush <= high_dayly) & (a.ta35 != 0) & (a.dte <= 40)].groupby(
            'timestamp').ta35.value_counts().reset_index()
        # Calculate the most common value for each 'timestamp' group
        most_common_values = a.groupby(a['timestamp'].dt.strftime('%H:%M:%S'))['ta35'].apply(
            lambda x: x.mode().iloc[0]).reset_index()
        most_common_values['ta35'] = round(
            most_common_values['ta35'].astype('int16'), -1)
        most_common_values['date'] = date
        most_common_values.to_parquet(path+'ta35/'+str(year)+'/'+str(date.date())+
                                      '.parquet')
        print(str(date.date()), 'added')