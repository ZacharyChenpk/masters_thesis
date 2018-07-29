#https://www.walletexplorer.com/wallet/Bittrex.com?format=csv
#https://www.walletexplorer.com/wallet/Bittrex.com/addresses
import pandas as pd
from multiprocessing import Pool

data = pd.read_csv('wexplorer.csv')
service = []
for i in range(len(data)):
    service.append(data.iloc[i]['Col'])


def scraping(service):
        pg_num = 1
        df = pd.DataFrame()
        while(1):
            url = 'https://www.walletexplorer.com/wallet/{}/addresses?page={}'.format(service,pg_num)
            d= pd.read_html(url,header= 0)

            if d[0].empty:
                break

            df = df.append(d[0])
            pg_num += 1

        df = df.reset_index()
        df.to_pickle("./data/{}".format(service))

pool = Pool(processes=64)
pool.map(scraping, service)
pool.close()
pool.join()
