from build_up_functions import *
from strategy_functions import *
pd.set_option('display.max_rows', 201)
pd.set_option('display.max_columns', None)
trades_path = "C:/Users/yschiff/OneDrive - Cisco/Desktop/FG3/Trades/"
ta_path= "C:/Users/yschiff/OneDrive - Cisco/Desktop/FG3/ta35/TA 35 Historical Data.csv"
ta35 = pd.read_csv(ta_path)
ta35['date'] = pd.to_datetime(ta35.date, infer_datetime_format=True)

a=[]
for year in range(2017,2018):
    trades = pd.read_parquet(trades_path+'trades'+str(year)+'.parquet')
    df = pd.merge(trades,ta35,on='date',how='left')


    df['close'] = round(df.close,-1)
    df =first_last(df)
    df.sort_values(by=['mispar_hoze','date','time'],inplace=True)

    ###
    cond_mimush = (df.close==df.mimush)
    cond_same_id =(df.mispar_hoze==df.mispar_hoze.shift(-1))
    cond_close = (df.open==0)
    close_trades_index = df.loc[ cond_mimush & cond_close & cond_same_id].index
    trades_index= close_trades_index.union(close_trades_index+1)
    df = df.loc[trades_index,:].reset_index(drop=True)

    df['p_open'] = np.where((df.open==0)&(df.mispar_hoze.shift(-1)==df.mispar_hoze),df.p.shift(-1),np.nan)
    df.rename(columns={'p':'p_close'},inplace=True)

    df = df.loc[df.open==0]

    df = df[['date','mispar_hoze','time','call',
    'mimush','p_close','p_open','days_left']]


    df['total'] = df.p_open - df.p_close 
    
    year = df.date.dt.year.iloc[0]

    for i in df.call.unique():
        print(i)
        df_one_kind = df.loc[df.call==i]
        # the most close to exipration  - less days left 
        df_one_kind = df_one_kind.loc[df_one_kind.groupby('date')['days_left'].idxmin()].reset_index(drop=True)
        type_option = df_one_kind.call.iloc[0]
        
        kind = 'Call' if type_option==1 else 'Put'
        total_long = df_one_kind.total.sum()
        n_trades = len(df_one_kind)
        df_one_kind['cum_return'] =  df_one_kind.total.cumsum()
        _max =  df_one_kind['cum_return'].max()
        _min =  df_one_kind['cum_return'].min()
        mean_buy = df_one_kind['p_close'].mean()
        mean_sell = df_one_kind['p_open'].mean()
        drawdown_nom = get_drawdown(df_one_kind)
        
        win_rate = len(df_one_kind.loc[df_one_kind.total>0])/ n_trades
        print(total_long)
        d = { 'year':  year,
        'kind' :kind,
        'total_long':total_long,
        'drawdown_nom' : drawdown_nom,
        'number_of_trades' : n_trades,
        'min_nominal' : _min,
        'max_nominal': _max,
        'win_rate' : win_rate,
        'mean sell': mean_sell,
        'mean buy' : mean_buy 

        }
        a.append(d)

results = pd.DataFrame(data=a)
results.to_csv('results_long_day_night_close.csv',index=False)


results









