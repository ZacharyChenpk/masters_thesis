import os
import pandas as pd
import py2neo
from collections import Counter
from functools import reduce
import pickle
import time
from collections import defaultdict

class User:
    def __init__(self, adrs, send_txs, rec_txs):
        self.adr = set(adrs)
        self.sending_tx = set(send_txs)
        self.cadr = set()
        if rec_txs is None:
            self.receiving_tx = set()
        else:
            self.receiving_tx = set(rec_txs)
        self.blocks = set()

def query_database(query):
    # REMEMBER TO BE CONNECTED TO IMPERIAL WIFI!
    graph_db = py2neo.Graph("https://dsi-bitcoin.doc.ic.ac.uk:7473/db/data/", auth=("adi", "aditi123"))
    return graph_db.run(query)

#CHANGE LATER TO QUERY DATABASE INSTEAD
def iadrs_from_tx(id_t, df):
    return set(df['iadr'][df["id_t"] == id_t])
#     query_string = """
#     MATCH (t:Tx) <-[:IN]- (txi:TxIn) <-[:UNLOCK]- (a:Address) WHERE ID(t) = {}
#     RETURN a.address as iadr
#     """.format(id_t)
#     x = query_database(query_string).to_data_frame()
#     return set(x['iadr'])

def oadrs_from_tx(id_t, df):
    return set(df['oadr'][df["id_t"] == id_t])
#     query_string = """
#     MATCH (t:Tx) -[:OUT]-> (txo:TxOut) -[:LOCK]-> (a:Address) WHERE ID(t) = 113001822
#     RETURN a.address as oadr
#     """.format(id_t)
#     x = query_database(query_string).to_data_frame()
#     return set(x['oadr'])

def tx_from_iadr(iadr, df):
    return set(df['id_t'][df["iadr"] == iadr])

def block_from_tx(id_t, df):
    return set(df['block_no'][df["id_t"] == id_t])

#FUNCTION TO WHICH YOU GIVE AN INPUT ADDRESS AND GET USER
def get_user(input_adr, df):
    to_inv = [input_adr]
    user_iadrs = set()
    seen_txs = set()
    while to_inv:
        current_iadr = to_inv.pop(0)
        user_iadrs.add(current_iadr)
        for id_t in tx_from_iadr(current_iadr, df):

            if id_t not in seen_txs:

                seen_txs.add(id_t)
                iadrs = iadrs_from_tx(id_t, df)
                to_inv += iadrs.difference(user_iadrs) #Adding addr
                user_iadrs.update(iadrs)

    return User(user_iadrs, seen_txs, None)

#TAKES OUTPUT ADDRESS AND GIVES USER THAT HAS THAT ADDRESS AS INPUT
def user_from_oadr(oadr, users):
    for i,user in enumerate(users):
        if oadr in user.adr:
            return i

def get_user_heur1(df):
    #LIST OF LISTS OF USER AND THEIR ASSOCIATED ADDRESSES
    users = []

    starttime = time.time()

    # Bitcoin-
    bitcoin = User({str(0)}, set(), set()) #Make user object with bitcoin iadr (which is 0)
    seen_miner_iadrs_tx = defaultdict(set) #Make dict associating miner payment address with tx_ids they've been involved in

    for index, row in df[df['iadr'] == str(0)].iterrows(): #Going through all mining txs
        bitcoin.sending_tx.add(row['id_t']) #Adding tx id to bitcoin user's txs
        seen_miner_iadrs_tx[row['oadr']].add(row['id_t']) # Updating dict to register the tx_id as corresponding to miners adr. If new, then new key added, otherwise added to values of existing key

    users.append(bitcoin) # Add bitcoin user
    already_seen_adr = {'0'}  # Bitcoin iadr has already been seen

    print("make miner users")
    # make miners users
    for adr, id_ts in seen_miner_iadrs_tx.items(): #Go through dictionary for every adr (miner) and txs he's been involved
        if adr not in already_seen_adr:
            miner = get_user(adr,df) #From an address, give back user ... aka identify all addresses belonging to miner
            miner.adr.add(adr) #Make sure adrs and txs are added in
            miner.receiving_tx.update(id_ts)
            users.append(miner)
            already_seen_adr.update(miner.adr)#Made sure miner's addresses are in already seen so that we don't create a second user with the same addresses

    print("make other users")
    ## ASSOCIATE INPUT ADDRESS AND TX WITH EACH USER IN BLOCK
    # make other users from heuristic
    for input_adr in df.iadr:
        if input_adr not in already_seen_adr:
            user = get_user(input_adr,df)
            users.append(user)
            already_seen_adr.update(user.adr)

    edges = defaultdict(int)

    print("Which users have transacted with each other")
    #WHICH USERS IN HAVE TRANSACTED WITH EACH OTHER
    for i,user in enumerate(users):
        for tx_id in user.sending_tx:
            for oadr in oadrs_from_tx(tx_id, df):
                if oadr in already_seen_adr:
                        edges[(i, user_from_oadr(oadr, users))]+=1

    print("Total time to process 1:", time.time()-starttime)

    return users, edges

def get_user_heur2(block_list, users, df):
    starttime = time.time()

    otc_dic = {}
    for block in block_list:
    #for block in range(first_block,last_block+1,1):
        if(os.path.exists("../pickles/df/{}.pickle".format(block)) and os.path.exists("../pickles/otc/otc_{}.pickle".format(block))):
            print("heuristic 2 for {}".format(block))
            with open ('../pickles/otc/otc_{}.pickle'.format(block), 'rb') as fp:
                otc_dic[block] = pickle.load(fp)
    not_seen = list(reduce(set.symmetric_difference, (set(val) for val in otc_dic.values())))

    appeared_once_o= list(df.drop_duplicates(['id_txo_out']).oadr.value_counts()[df.drop_duplicates(['id_txo_out']).oadr.value_counts()==1].index)
    all_iadrs= set(df.iadr)
    o_never_used_as_i = set(appeared_once_o).difference(all_iadrs)
    coin_gen_adr = list(df[df['iadr'] == str(0)].drop_duplicates(['oadr']).oadr)

    possible_otc = o_never_used_as_i.intersection(set(not_seen)).difference(set(coin_gen_adr))

    #Change Transactions
    for i,user in enumerate(users):
        cadrs_for_user = set()
        for tx_id in user.sending_tx:    #CHECK THIS
            o = oadrs_from_tx(tx_id, df)
            potential_cadr = []
            for oadr in o:
                if oadr in possible_otc and oadr not in iadrs_from_tx(tx_id, df):#not in user.adr:
                    potential_cadr.append(oadr)
            if len(potential_cadr)==1:
                cadrs_for_user.add(potential_cadr[0])
                #not_seen.remove(potential_cadr[0]) #Ensure that same change address won't be another user
        user.adr.update(cadrs_for_user)
        user.cadr.update(cadrs_for_user)

    print("Total time to process 2:", time.time()-starttime)
    return users


def getCouples(users):
    to_combine = []
    for i, user in enumerate(users):
        for k, otheruser in enumerate(users):
            if user.adr.intersection(otheruser.adr) and i!=k:
                to_combine.append((i,k))
    return to_combine

def assignBlocks(users, df):
    for user in users:
        blx = set()
        for tx in user.sending_tx:
            blx.update(block_from_tx(tx,df))
        for tx in user.receiving_tx:
            blx.update(block_from_tx(tx,df))
        user.blocks.update(blx)
    return users

def check_adrs_txs(users):
    tx_ids = []
    for user in users[1:]:#Drop out bitcoin user
        tx_ids += list(user.sending_tx)
        #tx_ids += list(user.receiving_tx)

    #tx_ids = list.append([list(user.sending_tx) for user in users])

    ads = []
    for i,user in enumerate(users):
        for ad in user.adr:
            ads.append(ad)

    cadrs = []
    for i,user in enumerate(users):
        for ad in user.cadr:
            cadrs.append(ad)

    try:
        if Counter(tx_ids).most_common(10)[0][1] == 1:  ##Repeated txids between bitcoin and the miners
            print("No repeats txid")

        if Counter(ads).most_common(10)[0][1] == 1:
            print("No repeats ads")

        if Counter(cadrs).most_common(10)[0][1] == 1:
            print("No repeats cadrs")
    except:
        pass

def construct_user_graph(df, users):
    #Construct User Graph
    df['input_user'] = df['iadr']
    df['output_user'] = df['oadr']

    starttime = time.time()
    #Replacing all input addresses and output addresses with a user corresponding to that address
    for i, user in enumerate(users):
        df['input_user'] = df['input_user'].apply(lambda x: i if x in user.adr else x)
        df['output_user'] = df['output_user'].apply(lambda x: i if x in user.adr else x)

    for tx_id, output_user in df[['id_t','output_user']].values:
        if isinstance(output_user,int):
            users[output_user].receiving_tx.add(tx_id)
    print("Total time to construct user graph:", time.time()-starttime)

    # can't trust input_val column now
    # because dropped lots of inputs
    edges_df0 = df.drop_duplicates(['input_user', 'id_txo_out'])
    edges_df = edges_df0.groupby(['input_user', 'output_user']).apply(lambda group: group['output_val'].sum()).reset_index()

    tups = [(input_user, output_user, amount) for (index, input_user, output_user, amount) in edges_df.itertuples()]

    return df, users, tups

def get_user_features_df(df, users):
    # user input features
    user_input_df = df.groupby('input_user').agg({
        'id_txo_out': 'nunique', #Num unique times paid out
        'oadr':'nunique', #Num of unique out addresses paid out
        'output_user': 'nunique', #Num of unique users paid out (Out Degree)
        #'id_txi': 'nunique', #Num unique times paid in
        'id_t': 'nunique', #Num Txs involved in
        'input_val': ['max', 'min']
    })

    user_input_df.columns = ['_'.join(col) for col in user_input_df.columns]

    user_input_df.rename(columns={
        'id_txo_out_nunique': 'unique_sent',
        'oadr_nunique': 'unique_sent_adr',
        'output_user_nunique': 'unique_sent_user',  # (Out Degree)
        'id_t_nunique': 'num_sending_tx',
        'input_val_max': 'max_sent',
        'input_val_min': 'min_sent'
    }, inplace=True)

    user_input_df['total_sent'] = (df['input_val'] / df['num_txo']).groupby(df['input_user']).sum()
    #dummy1 = (df['input_val'] / df['num_txo']).groupby(df['input_user']).sum()
    #dummy2 = (df['output_val'] / df['num_txi']).groupby(df['input_user']).sum()

    # user output features
    user_out_df = df.groupby('output_user').agg({
        'id_txi': 'nunique', #Num unique times paid in
        'iadr': 'nunique', #Num of unique in addresses paid this user
        'input_user': 'nunique', #Num of unique users paid in (In Degree)
        #'id_txo_out': 'nunique', #Num unique times paid
        'id_t': 'nunique', #Num Txs involved in
        'output_val': ['max', 'min']
    })

    user_out_df.columns = ['_'.join(col) for col in user_out_df.columns]

    user_out_df.rename(columns={
        'id_txi_nunique': 'unique_rec',
        'iadr_nunique': 'unique_rec_adr',
        'input_user_nunique': 'unique_rec_user',  # (In Degree)
        'id_t_nunique': 'num_receiving_tx',
        'output_val_max': 'max_rec',
        'output_val_min': 'min_rec'
    }, inplace=True)

    user_out_df['total_rec'] = (df['output_val'] / df['num_txi']).groupby(df['output_user']).sum()

    # Merge input and output user features
    user_df = user_input_df.merge(user_out_df, how='outer', left_index=True, right_index=True)
    user_df = user_df.iloc[:len(users)]

    # Name index
    user_df.index.name = 'user'

    # New columns
    user_df['num_tx'] = user_df['num_sending_tx'] + user_df['num_receiving_tx']
    # user_df = user_df.drop(['tx1', 'tx2'], axis=1)

    temp = df.groupby('output_user').agg({
        'iadr': lambda x: (x == '0').any(), #Num of unique in addresses paid this user
    })
    temp.rename(columns={
        'iadr': 'is_miner',
    }, inplace=True)

    user_df['is_miner'] = temp['is_miner'].iloc[:len(users)]
    # Fill in NA values
    user_df['is_miner'].fillna(False, inplace=True)
    user_df.is_miner = user_df.is_miner.apply(lambda x: 1 if x == True else 0)

    #user_df["category"] = "unknown"

    user_total_sent = [(user,total_rec) for (user, total_rec) in user_df['total_sent'].iteritems()]
    user_df = user_df.sort_index()

    _blocks = [len(_user.blocks) for _user in users]
    _num_adr = [len(_user.adr) for _user in users]

    user_df['num_blocks_active'] = _blocks
    user_df['num_adr'] = _num_adr

    user_df[['unique_sent_user_b','unique_sent_b', 'unique_sent_adr_b', 'num_sending_tx_b', 'total_sent_b', 'unique_rec_b', 'num_receiving_tx_b', 'unique_rec_user_b', 'unique_rec_adr_b', 'total_rec_b', 'num_tx_b']] = user_df[['unique_sent_user','unique_sent', 'unique_sent_adr', 'num_sending_tx', 'total_sent', 'unique_rec', 'num_receiving_tx', 'unique_rec_user', 'unique_rec_adr', 'total_rec', 'num_tx']].div(_blocks, axis=0)
    user_df[['total_sent_a', 'total_rec_a', 'num_tx_a']] = user_df[['total_sent', 'total_rec', 'num_tx']].div(_num_adr, axis=0)
    user_df[['total_sent_t', 'total_rec_t']] = user_df[['total_sent', 'total_rec']].div(user_df.num_tx, axis=0)

    user_df.fillna(0, inplace=True)
    user_df = user_df.iloc[1:] #Drop bitcoin user as it isn't really a 'user' of the network

    return user_df

def tag_users(users, user_df, df):
    #Dictionary structure -
    #'Address': 'Category'
    categories = ['exchanges','gambling','pool']
    dic_userlabels = defaultdict(set)
    starttime = time.time()

    all_adr = set(df.iadr).union(set(df.oadr))
    for category in categories:
        #for block in range(first_block,last_block+1,1):
        category_df = pd.read_pickle('../pickles/categories/{}.pickle'.format(category))
        x = all_adr.intersection(set(category_df.address))
        if x:
            for adr in x:
                dic_userlabels[adr].add(category)
    print("Total time to dict:", time.time()-starttime)

    user_label = defaultdict(set)
    for i, user in enumerate(users):
        labels = set()
        for key in dic_userlabels:
            if key in user.adr:
                cat = dic_userlabels[key]
                labels.add(list(cat)[0])
        if len(labels)==1:
            user_df.loc[i, 'category'] = list(cat)[0] #label[cat.pop()]
            user_label[i].update(labels)
        elif len(labels)>1:
            user_df.loc[i, 'category'] = 'mixed'
            with open('./out.txt', 'a') as f:
                print("user had multi tags {}".format(i), file=f)

    user_df = user_df.dropna(axis=0,how='any')

    if 'category' not in user_df.columns:
        print('tatti')
        user_df = user_df.iloc[0:0]

    return user_df, user_label, dic_userlabels
