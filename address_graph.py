import igraph
import py2neo
import pickle

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def query_database(query):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = py2neo.Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("guest_ro", "imperialO_nly"))
    return graph_db.run(query)

def get_block_data(first_block, last_block):
    query_string = """
                    MATCH (b:Block) <-[:MINED_IN]- (t:Tx) <-[:IN]- (txi:TxIn) <-[:UNLOCK]- (iadr:Address)
                    WHERE b.height >= {} AND b.height <= {}
                    MATCH (txi) <-[:SPENT]- (txo_in:TxOut)
                    MATCH (oadr:Address) <-[:LOCK]- (txo_out:TxOut) <-[:OUT]- (t)

                    RETURN iadr.address as iadr, oadr.address as oadr, txo_in.value as input_val, txo_out.value as output_val, ID(txo_in) as id_txo_in, ID(txi) as id_txi, ID(t) as id_t, ID(txo_out) as id_txo_out
                    """.format(first_block, last_block)
    return query_string

def address_graph(result,dic):
    tups1 = []
    for d in result:
        tups1.append((d['iadr'],d['oadr']))
    ig = igraph.Graph.TupleList(tups1,directed=True,vertex_name_attr='addr')
    for k in ig.vs:
        print(k)
        # if k in dic.keys():
        #     ig.es[0]["is_formal"] = True
        #     k['addr'] = dic[k]


with open('service_dic.pickle', 'rb') as handle:
    dic = pickle.load(handle)
result = query_database(get_block_data(400000,400000))
address_graph(result,dic)
