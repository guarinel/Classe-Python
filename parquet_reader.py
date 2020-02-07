import psycopg2
import pandas as pd
import numpy as np
import gc
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import timedelta, datetime
import os
import time
# from .parquet_creator import ParquetCreator
import asimov_database as ad


class ParquetReader:
    def __init__(self):
        self.RAM_AVAILABLE = 12
        self.creator = ad.ParquetCreator()

    def list_files(self):
        dict_files = {}
        dict_files['order-book'] = os.listdir('/bigdata/order-book')
        dict_files['level-book'] = os.listdir('/bigdata/level-book')
        dict_files['trades-book'] = os.listdir('/bigdata/trades')
        dict_files['events'] = os.listdir('/bigdata/events')
        return dict_files

    # Reader
    def get_marketdata_from_postgres(self, table, symbol, date, next_date=None, db='md_rt'):
        if table == 'incremental':
            return self.creator._load_postgres_data_for_incremental(symbol, date, next_date, db)
        elif table == 'trades':
            return self.creator._load_postgres_data_for_trades(symbol, date, next_date, db)
        return None
    
    def get_account_data_from_postgres(self, date, next_date=None):
        if len(date) == 10:
            date += ' 00:00:00'
        date = int(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").strftime("%s")) * 1000 * 1000 * 1000
        if next_date is not None:
            if len(next_date) == 10:
                next_date += ' 00:00:00'
            next_date = int(datetime.strptime(next_date, "%Y-%m-%d %H:%M:%S").strftime("%s")) * 1000 * 1000 * 1000
        return self.creator._load_order_mirror_data(date, next_date)

    def get_account_data_from_parquet(self, date, processed=None):
        path = '/bigdata/mirror/{}_processed.parquet'.format(date) if processed else '/bigdata/mirror/{}.parquet'.format(date)
        
        if os.path.isfile(os.getenv("HOME") + path):
            return pq.ParquetFile(os.getenv("HOME") + path)
        elif os.path.isfile(path):
            return pq.ParquetFile(path)
        else:
            print('File not found: ' + path)

    def get_parquet(self, symbol, date, type_='order-book'):

        type_ = '/bigdata/' + type_ + '/'

        if '/bigdata/trades/' in type_:
            if os.path.isfile(os.getenv("HOME") + type_ + "{}-{}.parquet".format(symbol, date)):
                return pq.ParquetFile(os.getenv("HOME") + type_ + "{}-{}.parquet".format(symbol, date))
            else:
                return pq.ParquetFile(type_ + "{}-{}.parquet".format(symbol, date))

        elif '/bigdata/events/' in type_:
            dict_data = {}
            incremental_file = "{}-incremental-{}.parquet".format(symbol, date)
            snapshot_file = "{}-snapshot-{}.parquet".format(symbol, date)
            if (os.path.isfile(os.getenv("HOME") + type_ + incremental_file) and 
                os.path.isfile(os.getenv("HOME") + type_ + snapshot_file)):
                dict_data['incremental'] = pq.ParquetFile(os.getenv("HOME") + type_ + incremental_file)
                dict_data['snapshot'] = pq.ParquetFile(os.getenv("HOME") + type_ + snapshot_file)
            else:
                dict_data['incremental'] = pq.ParquetFile(type_ + incremental_file)
                dict_data['snapshot'] = pq.ParquetFile(type_ + snapshot_file)
            return dict_data

        elif '/bigdata/order-book/' in type_ or '/bigdata/level-book/' in type_:
            dict_data = {}
            for side in ['bid', 'ask']:
                price_file = "{}-price-{}-{}.parquet".format(symbol, side, date)
                quantity_file = "{}-quantity-{}-{}.parquet".format(symbol, side, date)
                broker_file = "{}-broker-{}-{}.parquet".format(symbol, side, date)
                inc_code_file = "{}-inc_code-{}-{}.parquet".format(symbol, side, date)
                order_id_file = "{}-order_id-{}-{}.parquet".format(symbol, side, date)
                level_len_file = "{}-level_len-{}-{}.parquet".format(symbol, side, date)
                
                if (os.path.isfile(os.getenv("HOME") + type_ + price_file) and 
                    os.path.isfile(os.getenv("HOME") + type_ + quantity_file) and
                    os.path.isfile(os.getenv("HOME") + type_ + broker_file) and
                    os.path.isfile(os.getenv("HOME") + '/bigdata/order-book/' + inc_code_file) and
                    os.path.isfile(os.getenv("HOME") + '/bigdata/order-book/' + order_id_file)):
                    dict_data['{}_price'.format(side)] = pq.ParquetFile(os.getenv("HOME") + type_ + price_file)
                    dict_data['{}_quantity'.format(side)] = pq.ParquetFile(os.getenv("HOME") + type_ + quantity_file)
                    dict_data['{}_broker'.format(side)] = pq.ParquetFile(os.getenv("HOME") + type_ + broker_file)
                    dict_data['{}_inc_code'.format(side)] = pq.ParquetFile(os.getenv("HOME") + '/bigdata/order-book/' + inc_code_file)
                    dict_data['{}_order_id'.format(side)] = pq.ParquetFile(os.getenv("HOME") + '/bigdata/order-book/' + order_id_file)
                else:
                    dict_data['{}_price'.format(side)] = pq.ParquetFile(type_ + price_file)
                    dict_data['{}_quantity'.format(side)] = pq.ParquetFile(type_ + quantity_file)
                    dict_data['{}_broker'.format(side)] = pq.ParquetFile(type_ + broker_file)
                    dict_data['{}_inc_code'.format(side)] = pq.ParquetFile('/bigdata/order-book/' + inc_code_file)
                    dict_data['{}_order_id'.format(side)] = pq.ParquetFile('/bigdata/order-book/' + order_id_file)
               
                if os.path.isfile(os.getenv("HOME") + '/bigdata/level-book/' + level_len_file):
                    dict_data['{}_level_len'.format(side)] = pq.ParquetFile(os.getenv("HOME") + '/bigdata/level-book/' + level_len_file)
                else:
                    dict_data['{}_level_len'.format(side)] = pq.ParquetFile('/bigdata/level-book/' + level_len_file)

            return dict_data
        else:
            print('{} type not understood.'.format(type_))

    def get_level_price_data(self, symbol, date, columns=10):
        level_files = self.get_parquet(symbol, date, 'level-book')
        bid_cols = ['bid_{}'.format(i) for i in range(0, columns)]
        ask_cols = ['ask_{}'.format(i) for i in range(0, columns)]

        # First Read
        target_bid = level_files['bid_price'].read(columns=bid_cols, use_pandas_metadata=True).to_pandas()
        target_ask = level_files['ask_price'].read(columns=ask_cols, use_pandas_metadata=True).to_pandas()
        target_bid_inc = level_files['bid_inc_code'].read().to_pandas()
        target_ask_inc = level_files['ask_inc_code'].read().to_pandas()

        # Events
        target_events = self.get_parquet(symbol, date, 'events')
        target_events = target_events['incremental'].read().to_pandas()

        target_bid['msg_seq_num'] = target_events[target_events['side']!= 'A']['msg_seq_num'].values
        target_ask['msg_seq_num'] = target_events[target_events['side']!= 'B']['msg_seq_num'].values
        target_bid['event'] = target_events[target_events['side']!= 'A']['event_type'].values
        target_ask['event'] = target_events[target_events['side']!= 'B']['event_type'].values

        # Data manipulation
        target_data = pd.concat([target_bid, target_ask], sort=False).sort_index(kind='mergesort')
        target_inc = pd.concat([target_bid_inc, target_ask_inc], sort=False).sort_index(kind='mergesort')
        target_data['inc_code'] = target_inc.values
        target_data.index.name = 'index'
        target_data = target_data.sort_values(['index', 'inc_code'], kind='mergesort')

        target_indexes = target_events.groupby(['msg_seq_num']).last()['i'].values
        target_data = target_data.ffill()
        return target_data[target_data['inc_code'].isin(target_indexes)]

    def get_level_quantity_data(self, symbol, date, columns=10):
        level_files = self.get_parquet(symbol, date, 'level-book')
        bid_cols = ['bid_{}'.format(i) for i in range(0, columns)]
        ask_cols = ['ask_{}'.format(i) for i in range(0, columns)]

        # First Read
        target_bid = level_files['bid_quantity'].read(columns=bid_cols, use_pandas_metadata=True).to_pandas()
        target_ask = level_files['ask_quantity'].read(columns=ask_cols, use_pandas_metadata=True).to_pandas()
        target_bid_inc = level_files['bid_inc_code'].read().to_pandas()
        target_ask_inc = level_files['ask_inc_code'].read().to_pandas()

        # Events
        target_events = self.get_parquet(symbol, date, 'events')
        target_events = target_events['incremental'].read().to_pandas()

        target_bid['msg_seq_num'] = target_events[target_events['side']!= 'A']['msg_seq_num'].values
        target_ask['msg_seq_num'] = target_events[target_events['side']!= 'B']['msg_seq_num'].values
        target_bid['event'] = target_events[target_events['side']!= 'A']['event_type'].values
        target_ask['event'] = target_events[target_events['side']!= 'B']['event_type'].values

        # Data manipulation
        target_data = pd.concat([target_bid, target_ask], sort=False).sort_index(kind='mergesort')
        target_inc = pd.concat([target_bid_inc, target_ask_inc], sort=False).sort_index(kind='mergesort')
        target_data['inc_code'] = target_inc.values
        target_data.index.name = 'index'
        target_data = target_data.sort_values(['index', 'inc_code'], kind='mergesort')

        target_indexes = target_events.groupby(['msg_seq_num']).last()['i'].values
        target_data = target_data.ffill()
        return target_data[target_data['inc_code'].isin(target_indexes)]

    def get_quotes(self, symbol, date):
        df_level = self.get_level_price_data(symbol, date, columns=1)
        df_level = df_level[['bid_0', 'ask_0', 'msg_seq_num', 'event', 'inc_code']].rename(columns={'ask_0': 'ask_price', 'bid_0': 'bid_price'})

        df_level_quantity = self.get_level_quantity_data(symbol, date, columns=1)
        df_level_quantity = df_level_quantity[['bid_0', 'ask_0']].rename(columns={'ask_0': 'ask_quantity', 'bid_0': 'bid_quantity'})

        df_level['bid_quantity'] = df_level_quantity['bid_quantity']
        df_level['ask_quantity'] = df_level_quantity['ask_quantity']
        return df_level

    def get_sided_trades(self, symbol, date):
        pq_trades = self.get_parquet(symbol, date, type_='trades')
        df_trades = pq_trades.read().to_pandas()
        df_trades = df_trades.reset_index().set_index('msg_seq_num')
        df_trades = df_trades[~df_trades['crossed']]

        df_level = self.get_level_price_data(symbol, date, columns=1)
        df_level = df_level.set_index('msg_seq_num')[['bid_0', 'ask_0']].rename(columns={'ask_0': 'ask', 'bid_0': 'bid'})
        df_level = df_level[~df_level.index.duplicated()]
        df_level = df_level.shift(1)

        df = df_trades.join(df_level, how='outer')
        df[['bid', 'ask']] = df[['bid', 'ask']].ffill()
        df['deleted'] = False
        df = df.dropna(how='any')
        df['side'] = 2
        df.loc[df['price'] >= df['ask'], 'side'] = 0
        df.loc[df['price'] <= df['bid'], 'side'] = 1

        quantity_not_found = df[df.side == 2].shape[0]
        if quantity_not_found > 0:
            print('[ PARQUET ]', 'Trade side not found in {} trades'.format(quantity_not_found))

        return df.reset_index().set_index('ts')

    def get_aggregated_trades(self, symbol, date):
        trades = self.get_sided_trades(symbol, date)
        trades = trades.reset_index()

        agg_trades = trades.groupby('msg_seq_num').last()
        agg_trades['quantity'] = trades.groupby('msg_seq_num').agg({'quantity': 'sum'})['quantity']
        agg_trades['first_price'] = trades.groupby('msg_seq_num').agg({'price': 'first'})['price']
        agg_trades['count'] = trades.groupby('msg_seq_num').count()['symbol']
        agg_trades = agg_trades.reset_index()

        agg_trades['same'] = False
        agg_trades.loc[(agg_trades['count'] == 17) & (agg_trades['ts'] == agg_trades['ts'].shift(-1)) & ((agg_trades['buyer']
                                                                                                          == agg_trades['buyer'].shift(-1)) | (agg_trades['seller'] == agg_trades['seller'].shift(-1))), 'same'] = True

        for i in range(agg_trades.shape[0]-1, -1, -1):
            s = agg_trades.iat[i, -1]
            if not s:
                msn = agg_trades.iat[i, 0]
            else:
                agg_trades.iat[i, 0] = msn

        df = agg_trades.groupby('msg_seq_num').last()
        df['quantity'] = agg_trades.groupby('msg_seq_num').agg({'quantity': 'sum'})['quantity']
        df['first_price'] = agg_trades.groupby('msg_seq_num').agg({'price': 'first'})['price']
        del df['count'], df['same']
        return df.reset_index().set_index('ts')

    def get_available_dates(self, source='events', symbol=None):
        files = self.list_files()[source]
        files = [f.split('/')[-1].replace('.parquet', '') for f in files]
        if symbol is not None:
            files = [f for f in files if symbol in f]
        dates = [f.split('-') for f in files]
        dates = list(set(['{}-{}-{}'.format(d[-3], d[-2], d[-1]) for d in dates]))
        dates.sort()
        return dates

    def get_matching_dates(self):
        matching_dates = {}
        for month in ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']:
            for year in ['19']:
                dates = None
                for s in ['WDO', 'DOL']:
                    for source in ['events', 'trades-book']:
                        symbol = s + month + year
                        if dates != []:
                            dt = self.get_available_dates(source=source, symbol=symbol)
                            dates = dt if dates == None else [d for d in dt if d in dates]
                if dates is not None and len(dates) > 0:
                    matching_dates[month + year] = dates
        return matching_dates

    def _load_level_book_dol(self, symbol, date):
        self.date = date
        self.dol = symbol
    
        #Carrega e Trata os arquivos
        level_data = self.get_parquet(self.dol, self.date, "level-book")
        events = self.get_parquet(self.dol, self.date, "events")["incremental"].read().to_pandas()

        bid_price = level_data["bid_price"].read().to_pandas()
        bid_quantity = level_data["bid_quantity"].read().to_pandas()
        bid_inc_code = level_data["bid_inc_code"].read().to_pandas()

        ask_price = level_data["ask_price"].read().to_pandas()
        ask_quantity = level_data["ask_quantity"].read().to_pandas()
        ask_inc_code = level_data["ask_inc_code"].read().to_pandas()

        bid = bid_price[["bid_0"]].copy()
        bid_quantity.columns = ["bid_quantity_{}".format(i) for i in range(bid_quantity.shape[1])]
        bid["bid_quantity_0"] = bid_quantity["bid_quantity_0"]
        bid["i_bid"] = bid_inc_code[0]

        ask = ask_price[["ask_0"]].copy()
        ask_quantity.columns = ["ask_quantity_{}".format(i) for i in range(ask_quantity.shape[1])]
        ask["ask_quantity_0"] = ask_quantity["ask_quantity_0"]
        ask["i_ask"] =ask_inc_code[0]#.apply(lambda x: "A_" + str(x))

        events.reset_index(inplace=True)

        #Procura por atualizações provenientes da bolsa e retira da versão final
        valor_dol = np.setdiff1d(events['i'].values, bid_inc_code[0].values)

        retira_dol = np.setdiff1d(valor_dol,ask_inc_code[0].values)
        
        if len(retira_dol) != 0:
            for i in retira_dol:
                events = events[events['i'] != i]

        self.events = events

        for i in events.columns:
            bid[i] = events[events["side"] != "A"][i].values
            ask[i] = events[events["side"] != "B"][i].values

        ask['side'] = 'A'
        bid['side'] = 'B'


        df = pd.concat([ask, bid]).sort_values(by='i', kind = 'mergesort').reset_index()
        df['symbol'] = 'DOL'
        df.ffill(inplace=True)
        df.dropna(inplace=True)
        self.dol__ = df

        return df
        
    def _load_level_book_wdo(self, symbol, date):
        
        self.date = date
        self.wdo = symbol

        level_data_wdo = self.get_parquet(self.wdo, self.date, "level-book")
        events_wdo = self.get_parquet(self.wdo, self.date, "events")["incremental"].read().to_pandas()

        bid_price_wdo= level_data_wdo["bid_price"].read().to_pandas()
        bid_quantity_wdo = level_data_wdo["bid_quantity"].read().to_pandas()
        bid_inc_code_wdo = level_data_wdo["bid_inc_code"].read().to_pandas()

        ask_price_wdo = level_data_wdo["ask_price"].read().to_pandas()
        ask_quantity_wdo = level_data_wdo["ask_quantity"].read().to_pandas()
        ask_inc_code_wdo = level_data_wdo["ask_inc_code"].read().to_pandas()

        bid_wdo = bid_price_wdo[["bid_0"]].copy()
        bid_quantity_wdo.columns = ["bid_quantity_wdo_{}".format(i) for i in range(bid_quantity_wdo.shape[1])]
        bid_wdo["bid_quantity_wdo_0"] = bid_quantity_wdo["bid_quantity_wdo_0"]
        bid_wdo["i_bid_wdo"] = bid_inc_code_wdo[0]#.apply(lambda x: "B_" + str(x))
        bid_wdo.rename({'bid_0' : 'bid_wdo_0'}, axis = 1, inplace = True )

        ask_wdo = ask_price_wdo[["ask_0"]].copy()

        ask_wdo.rename({'ask_0':'ask_wdo_0'}, axis = 1, inplace= True)

        ask_quantity_wdo.columns = ["ask_quantity_wdo_{}".format(i) for i in range(ask_quantity_wdo.shape[1])]
        ask_wdo["ask_quantity_wdo_0"] = ask_quantity_wdo["ask_quantity_wdo_0"]
        ask_wdo["i_ask_wdo"] =ask_inc_code_wdo[0]

        events_wdo.reset_index(inplace=True)

        valor_wdo_bid = np.setdiff1d(events_wdo['i'].values, ask_inc_code_wdo[0].values)

        retira_wdo_bid = np.setdiff1d(valor_wdo_bid, bid_inc_code_wdo[0].values)

        if len(retira_wdo_bid) != 0:
            for i in retira_wdo_bid:
                events_wdo = events_wdo[events_wdo['i'] != i]
        
        # Busca os arquivos de eventos
        self.events_wdo = events_wdo


        #Cria os arquivos de bid e ask
        for i in events_wdo.columns:
            bid_wdo[i] = events_wdo[events_wdo["side"] != "A"][i].values
            ask_wdo[i] = events_wdo[events_wdo["side"] != "B"][i].values

        ask_wdo['side'] = 'A'
        bid_wdo['side'] = 'B'

        #Cria a versao final ja preenchendo os espacos vazios com os correspondentes valores
        dl = pd.concat([ask_wdo, bid_wdo]).sort_values(by='i', kind = 'mergesort').reset_index()
        dl['symbol'] = 'WDO'
        dl.ffill(inplace=True)
        dl.dropna(inplace=True)
        self.wdo__ = dl

        return dl 

    def load_level_book(self, symbol, date_):
        
        self.symbol = symbol 

        if type(self.symbol) is str:
            SYMBOL = list(self.symbol)
            if SYMBOL[0] == 'D':
                return self._load_level_book_dol(self.symbol, date_)
            elif SYMBOL[0] == 'W':
                return self._load_level_book_wdo(self.symbol, date_)
        else:
            if len(self.symbol) == 2:
                COD = list(self.symbol[0])
                dol_ = 'DOL' + COD[3] + COD[4] + COD[5]
                wdo_ = 'WDO' + COD[3] + COD[4] + COD[5]
                df = self._load_level_book_dol(dol_, date_)
                dl = self._load_level_book_wdo(wdo_, date_)
                total = pd.concat([df, dl]).sort_values(by='msg_seq_num', kind = 'mergesort').reset_index()
                total.ffill(inplace=True)
                total.dropna(inplace=True)
                total.drop({'level_0'}, axis=1, inplace=True)
                return total
            else: 
                SYMBOL = list(self.symbol[0])
                if SYMBOL[0] == 'D':
                    return self._load_level_book_dol(self.symbol, date_)
                elif SYMBOL[0] == 'W':
                    return self._load_level_book_wdo(self.symbol, date_)
                                 

if __name__ == '__main__':
    import asimov_database as ad
    e = ParquetReader()
    symbol = 'MRVE3'
    date = '2019-07-15'

    parquet = ad.ParquetCreator()
    parquet.create_book_events(symbol, date)
    parquet.create_trades_files(symbol, date)

    # Carrega dados de level-book, symbol pode ser lista ou str.
    dataframe = e.load_level_book(['WDOG20','DOLG20'], '2020-01-10')