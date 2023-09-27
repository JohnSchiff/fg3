
from build_up_functions import *
ta35 = pd.read_csv(path+'ta35/TA_35_Historical_Data.csv', parse_dates=['Date'])
ta35 = ta35.rename(columns={'Date': 'date'})

# loop over chosen years
for year in range(2022, 2023):

    # open new folder to store files of quotes which will be extracted
    create_folder(extract_path_Quotes)

    # get list of all Quotes files "/B" by year  which are zipped
    zip_Quotes_files = glob.glob(
        years_path+str(year)+'/MON_01/D23-31/B/*', recursive=True)
    # Check if there is Unzip files
    Check_unzip_files(zip_Quotes_files, extract_path=extract_path_Quotes)

    unzip_Quotes_files = Unzip_files(zip_Quotes_files, extract_path_Quotes)
    id_options_year_path = path + "id_options/id_options" + str(year)+'.csv'
    
    # dataframe of option's details
    df_options_id = pd.read_csv(id_options_year_path)
    
    for quotes_file in unzip_Quotes_files[-1:]:
        print(quotes_file)
        # YYYYMMDD, i.e 20211231 >> 31/12/21
        file_date = datetime.strptime(
            (quotes_file.split("FKL504R")[1][:8]), "%Y%m%d").strftime("%Y-%m-%d")            
        # reset index to have make it parsable
        df = pd.read_csv(quotes_file).reset_index()
        delete_file(quotes_file)
        df_quotes = quotes_parser(df)
        df_quotes = quotes_filter(df_quotes)
        df_quotes = quotes_merge_with_option_details(df_quotes, df_options_id)
        df_quotes = quotes_add_computed_ta35_index(df_quotes, ta35)
        # save file in YYYYMMDD format
        df_quotes.to_parquet(quotes_path+str(year)+'/'+file_date
                            + '.parquet', index=False)
