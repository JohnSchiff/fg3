

def remov_row(df):

    #Protection - remove rows where Buy isn't followed by Sell or vice versa
    cond_deals = ((df['open'] == 0) & (df['mispar_hoze'] == df['mispar_hoze'].shift(-1)) |
                (df['open'] == 1) & (df['mispar_hoze']
                                    == df['mispar_hoze'].shift(1))
                & ((df['Type'] != df['Type'].shift(-1)) | (df['Type'] != df['Type'].shift(1))))

    # Apply the protection
    df = df[cond_deals].sort_values(by=['mispar_hoze', 'timestamp'])

