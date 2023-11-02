
from strategy_classes import Open_close_strategy

a = Open_close_strategy(months=[12])

df = a.proccess_data()

result = a.final_result(df)
print(result.head())
