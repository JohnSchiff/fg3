import pandas as pd
import glob
import os
path = os.getcwd()

id_files_by_year = glob.glob('id_options/id_options*')

one_file = []
for file in id_files_by_year:
    df = pd.read_csv(file, index_col=[0])
    df['type'].replace({'C': 1, 'P': 0}, inplace=True)
    print(df['type'])
    df.to_csv(file, index=False)
#     one_file.append(df)

# result = pd.concat(one_file).drop_duplicates()

# result.to_csv('id_options/all_options_new.csv', index=False)
