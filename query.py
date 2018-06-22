import py2neo
import pandas
from sklearn.cluster import KMeans

import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def query_database(query):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = py2neo.Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("guest_ro", "imperialO_nly"))
    return graph_db.run(query)

def simple_query():
    query_string = """ 
                    MATCH (t:Tx) -[:MINED_IN]-> (b:Block) WHERE b.height=400000 RETURN b
                    """
    return query_string

def project_graph(algorithm, node, value):

    query_string = """
                    CALL algo.{}(
                    'MATCH ({}) RETURN id({}) AS id',
                    "MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target",
                    {graph: "cypher"})
                    """.format(algorithm, node, value)
    return query_string


result = query_database(simple_query())

result.data()


