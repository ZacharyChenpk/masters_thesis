from py2neo import Graph
from pandas import DataFrame
from sklearn.cluster import KMeans

import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

first_block = 364133
last_block = 364133


def query_database(first_block, last_block):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("guest_ro", "imperialO_nly"))
    query_string = """
                    MATCH (b:Block)<-[:MINED_IN] - (t:Tx)<-[in:IN] - (txi:TxIn)<-[u:UNLOCK] - (a:Address)
                    WHERE b.height<1000
                    MATCH (a)<-[l:LOCK]-(txo:TxOut)<-[out:OUT]-(t)
                    RETURN a, txi, t, txo LIMIT 8
                    """

    return graph_db.run(query_string).data()


result = query_database(first_block, last_block)
df = DataFrame(result)
df.to_csv('block_data.csv', encoding='utf-8', index=False)

# Convert DataFrame to matrix
mat = df.values
