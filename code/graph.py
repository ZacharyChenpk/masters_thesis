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

def address_graph(result,dic,users,top10users,top10colors):
    #tups1 = []
    #for d in result:
    #    tups1.append((d['iadr'],d['oadr']))
    #ag = igraph.Graph.TupleList(tups1,directed=True,vertex_name_attr='addr')
    ag = igraph.Graph.Read("../Graphs/400000_addr.graphml", format = "graphml")

    for i in ag.vs:
        
        ##CHANGE NAME TO ADDR
        a = i["name"]
        var = ''
        colored = False
        count = 0
        
        #Associate services from walletexplorer with addresses
        if a in dic.keys():
            i["service"] = dic[a]
        else:
            i["service"] = ""
        
#         #Associate addresses with users
#         for j, user in enumerate(users):
#             if a in user.adr or a in user.cadr:
#                 var = j
#                 assigned = True
#                 count = count +1

        #Associate addresses which belong top 10 users
        for j in top10users.keys():
            if a in users[j].adr or a in users[j].cadr:
                var = j
                i['color'] = top10colors[j]
                colored = True
                count = count +1
    
        if count>1:
            print("Something went wrong")
            
        i['user'] = str(var)
        if not colored:
            i['color'] = 'white'
            
    ag.write_graphml('./graphml/addr.graphml')
    print(ag.vertex_attributes())
    return ag

def user_graph(tups,dic,users,top10users,top10colors):
    ug = igraph.Graph.TupleList(tups,directed=True,vertex_name_attr='user',edge_attrs=['amount'])
    
    #Associate services from walletexplorer with users
    for u in ug.vs():
        all_addr = ''
        node = u['user']
        
        if node in list(range(len(users))):
            a = users[node].adr
            c = users[node].cadr
            for address in a.union(c):
                if address in dic.keys():
                    if all_addr != dic[address]:
                        all_addr += "{}".format(dic[address])
        elif node in dic.keys():
            all_addr += " {} ".format(dic[address])
        #print(all_addr)
        u['service'] = all_addr
#         if all_addr:
#             u['service'] = all_addr
#         else:
#             u['service'] = ''
    for i in ug.vs():
        if i['user'] in top10users.keys():
            i['top10'] = str(i['user'])
            i['size'] = top10users[i['user']]
            i['color'] = top10colors[i['user']]
            i['user'] = str(i['user'])
        else:
            i['top10'] = ''
            i['size'] = 10
            i['color'] = 'white'
            i['user'] = str(i['user'])
    ug.write_graphml('./graphml/user.graphml')
    print(ug.vertex_attributes())
    return ug

with open('./pickles/tups.pickle', 'rb') as handle:
    tups = pickle.load(handle)
with open('./pickles/service_dic.pickle', 'rb') as handle:
    dic = pickle.load(handle)
with open('./pickles/users.pickle', 'rb') as handle:
    users = pickle.load(handle)

#result = query_database(get_block_data(400000,400000))
result = 0    
    
count = []
for i, user in enumerate(users):
    count.append(tuple((len(user.adr)+len(user.cadr),i)))
count = sorted(count,reverse=True)

top10users = {}
for i in range(0,10):
    var1 = count[i][1]
    top10users[var1] = count[i][0]

colors = ['red', 'green', 'brown', 'yellow', 'crimson', 'pink', 'gold', 'orange', 'purple','cyan2']
top10colors = {}
for i in range(0,10):
    var1 = count[i][1]
    top10colors[var1] = colors[i]

new_ag = address_graph(result,dic,users,top10users, top10colors)
new_ug = user_graph(tups,dic,users,top10users, top10colors)
