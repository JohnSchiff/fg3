
##  this file is for manipulating all quotes files that we are using as database

from build_up_functions import *
from strategy_functions import *

# Make sure all data between trading hours 9:45 to 17:30
# end_time_input = "17:30:00"
# end_time = datetime.strptime(end_time_input, '%H:%M:%S').time()

# start_time_input = "09:45:00"
# start_time = datetime.strptime(start_time_input, '%H:%M:%S').time()

# Quotes files
files = glob.glob(path+'quotes/2022/*')

for file in files:
    # Read quotes file
    df = pd.read_parquet(file)
    df.drop(columns=['option_type','mimush','month','year','dte','ta35'],inplace=True)
    df.to_parquet(file, index=False)
    print(file)


# TODO flat all ta35 index when it is not 