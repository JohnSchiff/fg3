
##  this file is for manipulating all quotes files that we are using as database

from build_up_functions import *
from strategy_functions import *

# Make sure all data between trading hours 9:45 to 17:30
end_time_input = "17:30:00"
end_time = datetime.strptime(end_time_input, '%H:%M:%S').time()

start_time_input = "09:45:00"
start_time = datetime.strptime(start_time_input, '%H:%M:%S').time()

# Quotes files
files = glob.glob(path+'quotes/2021/*')

for file in files:
    # Read quotes file
    file_name = file[-18:-7]
    df = pd.read_parquet(file)
    before = len(df)
    df = df.loc[(df.timestamp.dt.time >= start_time) &
                (df.timestamp.dt.time <= end_time)]
    after = len(df)
    diff =  before - after
    if diff:
        print(f'diff :{diff} rows in  {file_name}')
        df.to_parquet(file,index=False)
        print(f'{file_name} saved')

