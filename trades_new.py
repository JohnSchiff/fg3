
from process_data_funcs import *

pkiya = pd.read_csv(path+'/files/pkiya.csv')
pkiya['pkiya'] = pd.to_datetime(pkiya['pkiya'], format='%d/%m/%Y')

# loop over 2015 to 2021
for year in range(2016, 2022):

    # empty list to put all trades into one
    l = []

    # create directory where all zip files by year  will be extracted to
    create_folder(extract_path_Trades)

    # get list of all Trades files ("/T") by year  which are zipped
    zip_Trades_files = glob.glob(years_path+str(year)+'/M*/D*/T/*', recursive=True)

    # Check if there is Unzip files
    Check_unzip_files(zip_Trades_files, extract_path=extract_path_Trades)

    # loop over all zipped files and extract them to extract path
    for zip_file in zip_Trades_files:
        # unzip trade file
        extract_single_rar_file(zip_file,extract_path_Trades)        
        trade_file = glob.glob(extract_path_Trades+'/*')[0]
        # All trades extract into one big dataframe
        df = filter_Trade_File(trade_file)
        # Delete csv file to save memory
        delete_file(trade_file)
        # append filtered trade files to a huge list
        l.append(df)

    # Convert the huge list to Dataframe
    all_year_trades = pd.concat(l, axis=0, ignore_index=True, sort=False)

    options = id_options(all_year_trades)
    options.merge(pkiya, on=['month', 'year']).to_csv(
        path+'/id_options/id_options'+str(year)+'.csv', index=False)

    # export Dataframe to parquet format files, saves 90% in memory
    all_year_trades.to_parquet(path+'/Trades/'
                               + 'trades'+str(year)
                               + '.parquet', index=False)


# Sanity check if exported file is equal to Dataframe
# suppose to be ok
df_parquet = pd.read_parquet(path+'/Trades/'+'trades'+str(year)+'.parquet')

if all_year_trades.equals(df_parquet):
    print('verificcation succeess')

else:
    print('Parquet file does not match!!!')
