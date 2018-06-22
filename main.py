import query
import pandas

import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

first_block = 364133
last_block = 364133

result = query.query_database(first_block, last_block)
df = pandas.DataFrame(result)
df.to_csv('block_data.csv', encoding='utf-8', index=False)

print('I did something too')
# Convert DataFrame to matrix
mat = df.values
