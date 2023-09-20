
def debug_quotes(files, date="2022-04-03", mispar_hoze=83863142):
    for file in files:
        if file.split('.parquet')[0][-10:] == date:
            print(file)
            df = pd.read_parquet(file)
            print(df.head())
            df = df.loc[df.mispar_hoze == mispar_hoze]
    return df
