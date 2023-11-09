from utils import *

class DataParser:
    def __init__(self, input_folder, extract_folder,files_type, year,path_to_save=quotes_path):
        self.input_folder = input_folder
        self.extract_folder = extract_folder
        self.files_type = files_type
        self.year = year
        self.path_to_save = path_to_save
        create_folder(self.extract_folder)

    def find_zip_files(self):
        zip_files = glob.glob(
            data_path + f'{self.year}/M*/D*/{self.files_type}/*', recursive=True)
        return zip_files
    
    def unzip_file(self, zip_file):
        extract_single_file(zip_file,self.extract_folder)
        files = glob.glob(self.extract_folder+'/*')
        return files
     
    def process_files_for_year(self):
        # Find all zip files for the given year
        zip_files = self.find_zip_files()
        # dataframe of options id
        df_options_id = self.df_options_per_year()
        # Iterate over each zip file
        for zip_file in zip_files:
            extracted_files = self.unzip_file(zip_file)
            for file in extracted_files:
                print(f'*************************************{file}***********************************')
                if self.files_type == 'B':
                    df = self.process_quotes_data(file, df_options_id)
                elif self.files_type == 'T':
                    df = self.process_trades_data(file, df_options_id)
                if df is False:
                    continue
                self.save_data(df)
                delete_file(file)
                # return df
                # Optionally, you can do something with the processed_data
                # ...

    def process_trades_data(self,file):
        print(file)
        # Specific processing for trade data
        # ...
        return 1

    def process_quotes_data(self, file, df_options_id):
        try:
            df = pd.read_csv(file).reset_index()
        except:
            return False
        if len(df) == 0:
            return False
        df_quotes = quotes_parser(df)
        df_quotes = quotes_filter(df_quotes)
        df_quotes = quotes_merge_with_option_details(df_quotes, df_options_id)
        df_quotes = quotes_add_computed_ta35_index(df_quotes, ta35)
        return df_quotes

    def save_data(self, df):
        date = str(df.timestamp.dt.date.iloc[0])
        df.reset_index(drop=True, inplace=True)
        df.to_parquet(self.path_to_save +str(self.year)+ '/' + date + '.parquet')
    
    def df_options_per_year(self):
        id_options_year_path = path + "id_options/id_options" + str(self.year)+ '.csv'
        df_options_id = pd.read_csv(id_options_year_path)
        return df_options_id
    
        


