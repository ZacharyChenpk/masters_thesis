import igraph
import py2neo
import pickle

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class User:
    def __init__(self, adrs, txs):
        self.adr = set(adrs)
        self.sending_tx = set(txs)
        self.cadr = set()
        self.receiving_tx = set(txs)

def query_database(query):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = py2neo.Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("adi", "aditi123"))
    return graph_db.run(query)

def get_block_data(first_block, last_block):
    query_string = """
                    MATCH (b:Block) <-[:MINED_IN]- (t:Tx) <-[:IN]- (txi:TxIn) <-[:UNLOCK]- (iadr:Address)
                    WHERE b.height >= {} AND b.height <= {}
                    MATCH (txi) <-[:SPENT]- (txo_in:TxOut)
                    MATCH (oadr:Address) <-[:LOCK]- (txo_out:TxOut) <-[:OUT]- (t)

                    RETURN iadr.address as iadr, oadr.address as oadr
                    """.format(first_block, last_block)
    return query_string

def address_graph(result,dic,users):
    tups1 = []
    for d in result:
        tups1.append((d['iadr'],d['oadr']))
    ag = igraph.Graph.TupleList(tups1,directed=True,vertex_name_attr='addr')

    for i,k in enumerate(ag.vs):
        a = ag.vs[i]["addr"]
        assigned = False
        if a in dic.keys():
            ag.vs[i]["Label"] = dic[a]
        else:
            ag.vs[i]["Label"] = ""
        for j, user in enumerate(users):
            if a in user.adr or a in user.cadr:
                ag.vs[i]["addr"] = j
                assigned = True
        if not assigned:
            ag.vs[i]["addr"] = a

    ag.write_graphml('./graphml/addr.graphml')

def user_graph(tups):
    ug = igraph.Graph.TupleList(tups,directed=True,vertex_name_attr='user')#,edge_attrs=['amount'])

    for u in ug.vs():
        all_addr = []
        node = u['user']
        if node in list(range(len(users))):
            a = users[node].adr
            c = users[node].cadr
            for address in a.union(c):
                if address in dic.keys():
                    all_addr.append(address)
                    check = True
        elif node in dic.keys():
            all_addr.append(address)

        if all_addr:
            node = all_addr
        else:
            node = ''

    print(ug.vertex_attributes())

    ug.write_graphml('./graphml/user.graphml')

with open('./pickles/tups.pickle', 'rb') as handle:
    tups = pickle.load(handle)
with open('./pickles/service_dic.pickle', 'rb') as handle:
    dic = pickle.load(handle)
with open('./pickles/users.pickle', 'rb') as handle:
    users = pickle.load(handle)
result = query_database(get_block_data(400000,400000))
address_graph(result,dic,users)
user_graph(tups)
