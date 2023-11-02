import pandas as pd
import numpy as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def Graph_performance(df,path,low_diff,days_letf_min,days_letf_max):

    TA35_close = pd.read_csv(path+'TA35_yahoo_high_low.csv')
    Date(TA35_close,date_column='Date')
    # Fix datetime
    Date(df,date_column='date')


    fig, (ax1, ax2, ax3) = plt.subplots(3, figsize=(30,10))
    fig.subplots_adjust(hspace=0.5)
    # ax1.set_xlabel("Date", fontsize=10)
    ax1.set_ylabel("Nominal Return",fontsize=10)
    ax1.set_title("Nominal Return  diffrence from madad is "+
    str (low_diff)+' points, days to Exipration from '+str(days_letf_min)+' to '+str(days_letf_max),
    fontsize=12)
    ax2.set_ylabel("Madad TA35",fontsize=10)

    ax3.set_ylabel("Return in Percentages",fontsize=10)
    ax3.set_title(" Return Normalized in pct diffrence from madad is "+
    str (low_diff)+' points,  days to Exipration from '+str(days_letf_min)+' to '+str(days_letf_max),
    fontsize=12)


    ax1.plot(df.date,df.nominal_sum,color='g')
    ax2.plot(TA35_close.Date,TA35_close.Close,color='r',label='Madad Tel Aviv 35')
    ax3.plot(df.date,df.normal_return,color='b')

    ax1.grid(True)
    ax2.grid(True)
    ax3.grid(True)

    ax2.legend()
    
    # format is very important , if not it doesn't save
    plt.savefig(path+'Graphs/'+'days_left_from '+str(days_letf_min)+' to '+
    str(days_letf_max)+' Diff from madad is ' +str(low_diff)+'points'+'.png',format='png') 


    return('Graph was build and exported in' + path)

######################
def graph_nominal_return(df,path):
        Date(df)
        plt.figure(figsize=(10,5))
        plt.grid(True)
        plt.xlabel("Date", fontsize=10)
        plt.ylabel("Return",fontsize=10)
        plt.title("Nominal Return over years", fontsize=7)
        plt.plot(df['date'],df['nominal_sum'])
        locator = mdates.YearLocator()
        # plt.gca().xaxis.set_major_locator(locator)
        # plt.gcf().autofmt_xdate()
        plt.savefig(path+'nominal_Return.png')

def graph_normalized_return(df,path):
        Date(df)
        plt.figure(figsize=(10,5))
        plt.grid()
        plt.xlabel("Date", fontsize=10)
        plt.ylabel("Return",fontsize=10)
        plt.title("Normalized Return over years", fontsize=7)
        plt.plot(df.date,df.normal_return)
        locator = mdates.YearLocator()
        plt.gca().xaxis.set_major_locator(locator)
        plt.gcf().autofmt_xdate()
        plt.savefig(path+'noramlized_Return.png')

