from functions import *
from multiprocessing import Pool
import time
# import warnings
# warnings.simplefilter('error', RuntimeWarning)
initaltime = time.time()

def generate(i):
    df = pd.DataFrame()
    block_list = []
    for block in range(i,i+10):
        if(os.path.exists("../pickles/df/{}.pickle".format(block)) and os.path.exists("../pickles/otc/otc_{}.pickle".format(block))):
            block_list.append(block)
            temp_df = pd.read_pickle("../pickles/df/{}.pickle".format(block))
            if temp_df.empty:
                print('oh no')
            temp_df['block_no']=block
            df = df.append(temp_df)
    df = df.reset_index()
    # New columns for number of input and output transaction ids
    df['num_txo'] = df.groupby('id_t')['id_txo_out'].transform('nunique')
    df['num_txi'] = df.groupby('id_t')['id_txi'].transform('nunique')

    users = get_user_heur1(df)

    users = get_user_heur2(block_list, users, df)

    users = assignBlocks(users, df)

    # to_combine = getCouples(users)
    # while to_combine:
    #     print("to_combine length ", len(to_combine))
    #     st = set(users)
    #     for tple in to_combine:
    #         user1 = users[tple[0]]
    #         user2 = users[tple[1]]
    #         if user1 in st and user2 in st:
    #             #print(tple)
    #             st.remove(user1)
    #             st.remove(user2)
    #             user1.adr = user1.adr.union(user2.adr)
    #             st.add(user1)
    #     users = list(st)
    #     print(len(users))
    #     to_combine = getCouples(users)
    #
    # check_adrs_txs(users)

    df, users = construct_user_graph(df, users)

    user_df = get_user_features_df(df, users)

    user_df = tag_users(users, user_df, df)

    user_df.to_pickle("./data/user_df_labelled_{}.pickle".format(i))

pool = Pool(processes=64)
pool.map(generate, list(range(400510,401380,10)))
pool.close()
pool.join()

print("Total time to process: {}".format(time.time()-initaltime))
