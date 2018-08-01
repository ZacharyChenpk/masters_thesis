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
                    MATCH (txi) <-[:SPENT]- (txo_in:TxOut) 
                    MATCH (oadr:Address) <-[:LOCK]- (txo_out:TxOut) <-[:OUT]- (t)
                    
                    RETURN iadr.address as iadr, oadr.address as oadr, txo_in.value as input_val, txo_out.value as output_val, ID(txo_in) as id_txo_in, ID(txi) as id_txi, ID(t) as id_t, ID(txo_out) as id_txo_out
                    """.format(first_block, last_block)
    return query_string

def get_transaction_degrees(first_block, last_block):
    query_string = """
                    MATCH (b:Block) <-[:MINED_IN]- (t:Tx)
                    WHERE b.height >= {} AND b.height <= {}
                    RETURN id(t) as id_tx, apoc.node.degree(t,'IN') as degree_in, apoc.node.degree(t,'OUT') as degree_out
    """.format(first_block,last_block)
    return query_string

def get_coinbase(first_block, last_block):
    query_string = """
                    MATCH (b:Block) <-[:MINED_IN]- (t:Tx)<-[:IN]-(cb:CBscript)
                    MATCH (oadr:Address) <-[:LOCK]- (txo_out:TxOut) <-[:OUT]- (t)
                    WHERE b.height >= {} AND b.height <= {}
                    RETURN oadr.address as oadr, txo_out.value as output_val, ID(cb) as id_txi, ID(t) as id_t, ID(txo_out) as id_txo_out
    """.format(first_block,last_block)
    return query_string

def write_to_csv(result,string):

    df = result.to_data_frame()

    if (df.empty):
        print("Something went wrong, there is no data for this/these blocks")
    else:

        df.to_csv('{}.csv'.format(string), encoding='utf-8', index=False)


