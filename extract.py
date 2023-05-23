import os
import sys
import time
import numpy as np
import pandas as pd

def compressed_snapshot(bids, asks, precision):
    best_bid = bids.index.max()
    best_ask = asks.index.min()
    mid = (best_bid + best_ask)/2
    df = pd.concat(
                        [
                        bids.rename('amount').reset_index() \
                            .assign(side = 'bid'),
                        asks.rename('amount').reset_index() \
                            .assign(side = 'ask')
                        ]) \
            [lambda x: np.abs((x['price'] - mid)/mid) < precision*100] \
            .assign(price_bucketed = lambda x: pd.cut(x['price'],bins = 200).apply(lambda y:y.mid)) \
            .groupby(['side','price_bucketed'])['amount'].sum().reset_index()
    return df

def extract_orderbooks(name, input_folder, output_folder):
    precisions = [1e-3,1e-4]
    if os.path.exists(os.path.join(output_folder,f'{name}_compressed_10bps.csv')):
        return False
    orderbook_updates = pd.read_csv(os.path.join(input_folder,f'{name}.csv'))
        
    # orderbook must be reinitialized at each snapshot + 1m rows limit not to saturate memory with no 
    batching_reset = pd.DataFrame({ 
            **{index: True 
            for index in orderbook_updates[lambda x: x['is_snapshot'] & ~(x['is_snapshot'].shift(1).fillna(True))].index},
            **{i: False for i in range(0,len(orderbook_updates),int(1e7))
                        if i != 0},
            **{0: True, len(orderbook_updates):False}
                                }, index = ['need_reset']).T.sort_index()

    res = {precision:[] for precision in precisions}
    for i in range(len(batching_reset) - 1):
        #create coherent update batch
        if batching_reset.iloc[i]['need_reset']:
            update_batch = orderbook_updates[batching_reset.index[i]:batching_reset.index[i + 1]]
        else:
            update_batch = pd.concat(
                                [
                                    last_updates,
                                    orderbook_updates[batching_reset.index[i]:batching_reset.index[i + 1]]
                                ]
                                )
        
        #reconstruct orderbook with batch
        most_recent_orders = update_batch \
            .assign(minute = lambda x: np.ceil(x['timestamp']/6e7)*6e7) \
            .groupby(['minute','side','price'])['amount'].last() \
            .unstack(['side','price']).ffill().replace(0,np.NaN)
        
        #compress reconstructed orderbook
        for minute in most_recent_orders.index:
            asks = most_recent_orders.loc[minute].loc['ask'].dropna()
            bids = most_recent_orders.loc[minute].loc['bid'].dropna()
            for precision in precisions:
                res[precision].append(compressed_snapshot(bids, asks, precision).assign(timestamp = minute))
        
        last_minute = most_recent_orders.index[-1]
        last_updates = pd.concat([asks.rename('amount').reset_index() \
                                        .assign(local_timestamp = last_minute,
                                                side = 'ask'),
                                bids.rename('amount').reset_index() \
                                        .assign(local_timestamp = last_minute,
                                                side = 'bid')])

    # Store results
    for precision in precisions:
        orderbook_compressed = pd.concat(res[precision])
        orderbook_compressed.to_csv(os.path.join(output_folder,f'{name}_compressed_{int(1e4*precision)}bps.csv'))
    return True

if __name__ == '__main__':
    start = time.time()
    name = sys.argv[1]
    input_folder = os.path.join(sys.path[0], 'input')
    output_folder = os.path.join(sys.path[0], 'output')
    result = extract_orderbooks(name,input_folder,output_folder)
    end = time.time()
    if result:
        sys.exit(f"Extraction of {name} done in {int(end - start)} seconds")
    else:
        sys.exit(f"Extraction of {name} already done")