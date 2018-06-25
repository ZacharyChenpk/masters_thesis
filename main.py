import query
import pandas

first_block = 400000
last_block = 400000

result = query.query_database(query.get_block_data(first_block,first_block))

if(result is None):
    print("Something went wrong, there is no data for this/these blocks")

df = result.to_data_frame()
df.to_csv('block_data.csv', encoding='utf-8', index=False)

print(df.columns[0])

