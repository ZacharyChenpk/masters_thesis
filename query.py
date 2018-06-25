import py2neo
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def query_database(query):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = py2neo.Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("guest_ro", "imperialO_nly"))
    return graph_db.run(query)

def check_block(first_block, last_block):
    query_string = """ 
                    MATCH (b:Block) 
                    WHERE b.height >= {} AND b.height <= {}
                    RETURN b
                    """.format(first_block, last_block)
    return query_string

def get_block_data(first_block, last_block):
    query_string = """
                    MATCH (b:Block) <-[:MINED_IN]- (t:Tx) <-[:IN]- (txi:TxIn) <-[:UNLOCK]- (iadr:Address)
                    WHERE b.height >= {} AND b.height <= {}
                    MATCH (txi) <-[:SPENT]- (txo1:TxOut) 
                    MATCH (oadr:Address) <-[:LOCK]- (txo2:TxOut) <-[:OUT]- (t)
                    RETURN iadr.address, txo1.value, ID(txo1), ID(txi), ID(t), ID(txo2), txo2.value, oadr.address
                    """.format(first_block, last_block)
    return query_string



