import psycopg2
import pandas as pd
import numpy as np
import gc
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timedelta
import os
import time
import asimov_database as ad

class ParquetCreator:
    def __init__(self, ram_memory=16):
        self.RAM_AVAILABLE = ram_memory
        self.conn = None
        self.valid_dates = {}
        self.valid_database = None
        self.consult = ad.Consultor()

    def _create_sql_connection(self, database='md_rt'):
        count = 0
        while count < 2 and (self.conn is None or self.valid_database != database):
            self.valid_database = database
            try:
                params = {
                    'database': database,
                    'user': 'asimov',
                    'password': 'asimov',
                    'host': '10.112.1.20' if not database in ['asimov_mirror', 'asimov_dropcopy'] else '10.243.7.7',
                    'port': 5432 ,
                    }
                self.conn = psycopg2.connect(**params, connect_timeout=2)
            except:
                print(' [ PARQUET ] Error on connection creation. Trying again...')
                count += 1
                time.sleep(1)
                self.conn = None
        
    def _valid_connection(self):
        if self.conn is None:
            print(' [ PARQUET ] Invalid connection to database')
            return False
        return True

    def _load_postgres_data_for_incremental(self, symbol, date, next_date=None, db='md_rt'):
        self._create_sql_connection(db)

        date = date + ' 00:00:00' if len(date) < 12 else date
        next_date = (datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") if next_date is None else next_date
        string_ = "SELECT * FROM {} WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}'".format('md_incremental', date, next_date, symbol)   
        if not self._valid_connection():
            return (pd.DataFrame(), pd.DataFrame())
        md_incremental = pd.read_sql_query(string_, self.conn)
        if len(md_incremental) > 0:
            md_incremental.symbol = md_incremental.symbol.astype(str)
            md_incremental.event_type = md_incremental.event_type.astype(str)
            md_incremental.side = md_incremental.side.astype(str)
            md_incremental.order_ts = md_incremental.order_ts.astype(str)
            md_incremental.status = md_incremental.status.astype(str)

            md_incremental.index = pd.to_datetime(md_incremental.ts, utc=True)
            del md_incremental['ts']
            md_incremental = md_incremental.sort_values(by=['id'])
            md_incremental['i'] = np.arange(0, md_incremental.shape[0])

        string_ = "SELECT * FROM {} WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}'".format('md_snapshot', date, next_date, symbol)
        md_snapshot = pd.read_sql_query(string_, self.conn)
        if len(md_snapshot) > 0:
            md_snapshot.index = pd.to_datetime(md_snapshot.ts, utc=True)
            del md_snapshot['ts']
            md_snapshot.sort_values(['msg_seq_num', 'position'], inplace=True)

        return (md_incremental, md_snapshot)

    def _load_postgres_data_for_trades(self, symbol, date, next_date=None, db='md_rt'):
        self._create_sql_connection(db)

        date = date + ' 00:00:00' if len(date) < 12 else date
        next_date = (datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") if next_date is None else next_date
        string_ = "SELECT * FROM {} WHERE ts >= '{}' AND ts < '{}' AND symbol = '{}'".format('md_trade', date, next_date, symbol)
        if not self._valid_connection():
            return pd.DataFrame()
        md_trades = pd.read_sql_query(string_, self.conn)
        if len(md_trades) > 0:
            md_trades.index = pd.to_datetime(md_trades.ts, utc=True)
            del md_trades['ts']
            md_trades = md_trades.sort_values(by=['trade_id'])
        return md_trades
    
    def _load_order_mirror_data(self, date, next_date=None, db='asimov_mirror'):
        self._create_sql_connection(db)

        next_date = date + 60 * 60 * 24 * 1000 * 1000 * 1000 if next_date is None else next_date
        string_ = "SELECT * FROM {} WHERE ts >= '{}' AND ts < '{}'".format('order_mirror', date, next_date)
        if not self._valid_connection():
            return pd.DataFrame()
        md_mirror = pd.read_sql_query(string_, self.conn)
        return md_mirror
    
    def _load_dropcopy_trades(self, date, next_date=None, db='asimov_dropcopy'):
        self._create_sql_connection(db)

        date = date + ' 00:00:00' if len(date) < 12 else date
        next_date = (datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") if next_date is None else next_date
        string_ = "SELECT * FROM {} WHERE timestamp >= '{}' AND timestamp < '{}'".format('"TRADE"', date, next_date)
        if not self._valid_connection():
            return pd.DataFrame()
        md_mirror = pd.read_sql_query(string_, self.conn)
        return md_mirror
    
    def _load_dropcopy_position(self, db='asimov_dropcopy'):
        self._create_sql_connection(db)
        string_ = "SELECT * FROM {}".format('"POSITION"')
        if not self._valid_connection():
            return pd.DataFrame()
        md_mirror = pd.read_sql_query(string_, self.conn)
        return md_mirror

    def _save_parquet(self, where, data, ns_index=False):
        data_to_write = pa.Table.from_pandas(data)
        writer = pq.ParquetWriter(where, data_to_write.schema, use_deprecated_int96_timestamps=ns_index)
        writer.write_table(data_to_write)
        writer.close()

    # Data creators
    def create_book_events(self, symbol, date, db="md_rt"):
        try:
            md_incremental, md_snapshot = self._load_postgres_data_for_incremental(symbol, date, db=db)

            if len(md_incremental) > 0:
                print('[ PARQUET ] Fetching incremental data for', symbol, date)
                self._save_parquet(f"/bigdata/events/{symbol}-incremental-{date}.parquet", md_incremental)
                print('[ PARQUET ] Fetching snapshot data for', symbol, date)
                self._save_parquet(f"/bigdata/events/{symbol}-snapshot-{date}.parquet", md_snapshot)
 
            else:
                print('[ PARQUET ] Data not found in md_incremental', symbol, date)
        
        except Exception as e:
            print('[ PARQUET ] Error in create_book_events', symbol, date, e)

    def create_trades_files(self, symbol, date, db="md_rt"):
        
        try:
            print('[ PARQUET ] Fetching trades data for', symbol, date)
            md_trades = self._load_postgres_data_for_trades(symbol, date, db=db)

            if len(md_trades) > 0:
                self._save_parquet(f"/bigdata/trades/{symbol}-{date}.parquet", md_trades)

            else:
                print('[ PARQUET ] Data not found in md_trade', symbol, date)

        except Exception as e:
            print('[ PARQUET ] Error in create_trades_files', symbol, date, e)

    def create_mirror_files(self, date , db='asimov_mirror'):
        since = int(datetime.strptime(date, "%Y-%m-%d").strftime("%s")) * (1000 ** 3)
        until = (int(datetime.strptime(date, "%Y-%m-%d").strftime("%s")) + 24 * 60 * 60) * (1000 ** 3)
        md_mirror =  self._load_order_mirror_data(since, until, db=db)
        if len(md_mirror) > 0:
            self._save_parquet(f"/bigdata/mirror/{date}.parquet", md_mirror)
    
    def create_processed_mirror_files(self, date): # , db='asimov_mirror'):
        from asimov_tools.mirror.mirror import Mirror
        mirror = Mirror(date)
        if len(mirror.orders) > 0:
            if os.path.isdir(os.getenv("HOME") + "/bigdata"):
                self._save_parquet(os.getenv("HOME") + "/bigdata/mirror/{date}_processed.parquet", mirror.orders, True)
            else:
                self._save_parquet(f"/bigdata/mirror/{date}_processed.parquet", mirror.orders, True)

    def create_all_trade_files_for_symbol(self, symbol, overwrite=True):
        dates = self.get_valid_dates(symbol, 'md_trade')
        created = self.get_created_dates_for_symbol(symbol, 'trades')
        print(' [ PARQUET ] Creating trade files for dates: ', ', '.join(dates))
        for date in dates:
            if overwrite:
                self.create_trades_files(symbol, date)
            else:
                if not date in created:
                    self.create_trades_files(symbol, date)
    
    def create_all_book_events_files_for_symbol(self, symbol, overwrite=True):
        dates = self.get_valid_dates(symbol, 'md_incremental')
        created_inc = self.get_created_dates_for_symbol(symbol, 'incremental')
        created_snap = self.get_created_dates_for_symbol(symbol, 'snapshot')
        created = [d for d in created_inc if d in created_snap]
        print(' [ PARQUET ] Creating book_events files for dates: ', ', '.join(dates))
        for date in dates:
            if overwrite:
                self.create_book_events(symbol, date)
            else:
                if not date in created:
                    self.create_book_events(symbol, date)

    def create_order_book_files(self, symbol, date, db='md_rt'):
        md_incremental, md_snapshot = self._load_postgres_data_for_incremental(symbol, date, db=db)
        # Se não há STARTED no dia, return
        if 'OPEN' not in md_incremental['status'].values:
            return

        # Altera order ids para indexes
        md_incremental['order_id'] = md_incremental['order_id'].fillna(0)
        dict_ = {j:i  for i, j in enumerate(md_incremental['order_id'].unique())}
        md_incremental['order_id'] = md_incremental['order_id'].apply(lambda x: dict_[x]).astype(np.float32)

        md_incremental['inc_code'] = np.arange(md_incremental.shape[0])
        md_incremental['acum'] = 0
        md_incremental['acum'][md_incremental['event_type'] == 'INSERT'] = 1
        md_incremental['acum'][md_incremental['event_type']== 'DELETE'] = -1
        md_incremental['acum'][md_incremental['event_type'] == 'DELETE_FROM'] = -md_incremental['position'][md_incremental['event_type'] == 'DELETE_FROM']
        md_incremental_data = {'bid': md_incremental[md_incremental['side']!='A'], 'ask': md_incremental[md_incremental['side']!='B']}
        col_map = {md_incremental.columns[i]: i for i in range(len(md_incremental.columns))}

        # Check se há snapshots para tratar
        if len(md_snapshot):
            snap_type = True
            idx_snap = md_snapshot.index.unique()
        else:
            snap_type = False

        c = 0
        for side in md_incremental_data.keys():
            price_file = f"/bigdata/order-book/{symbol}-price-{side}-{date}.parquet"
            quantity_file = f"/bigdata/order-book/{symbol}-quantity-{side}-{date}.parquet"
            broker_file = f"/bigdata/order-book/{symbol}-broker-{side}-{date}.parquet"
            inc_code_file = f"/bigdata/order-book/{symbol}-inc_code-{side}-{date}.parquet"
            order_id_file = f"/bigdata/order-book/{symbol}-order_id-{side}-{date}.parquet"

            print('Working on {} {} data'.format(side, symbol))
            np_incr = md_incremental_data[side].values.copy()
            ts_index = md_incremental_data[side].index.copy()

            max_levels = int(max(md_incremental_data[side]['position'].max(), md_incremental_data[side]['acum'].cumsum().max())) + 10
            matrix_size = (max_levels *  4 *  np_incr.shape[0] * 4) / (1024 * 1024 * 1024)
            splits = int(matrix_size / self.RAM_AVAILABLE * 4) + 1
            list_inc_code = []

            # Splitting data to fit RAM
            split_size = int(np_incr.shape[0] / splits) + 1
            np_data = np.zeros((max_levels, 3, split_size), dtype=np.float32)
            np_order_id = np.zeros((max_levels, split_size), dtype=np.float32)

            j = 0
            order_book_index = []
            msn_to_ignore = 0

            for i in range(len(np_incr)):
                # IGNORE MSG_SEQ_NUM
                if np_incr[i, col_map['msg_seq_num']] == msn_to_ignore:
                    if np_incr[i, col_map['event_type']] != 'STATUS' and np_incr[i, col_map['event_type']] != 'STARTED': 
                        continue

                if j > 0:
                    np_data[:, :, j] = np_data[:, :, j-1]
                    np_order_id[:, j] = np_order_id[:, j-1]

                # =========================================================================
                try:
                    pos = int(np_incr[i, col_map['position']]) - 1
                except:
                    pos = 0

                # SNAPSHOT
                if snap_type and ts_index[i] in idx_snap and np_incr[i, col_map['event_type']] == 'STARTED':
                    print('Working on snapshot data')
                    msn_to_ignore = np_incr[i, col_map['msg_seq_num']]
                    snap_side = 'B' if side == 'bid' else 'A'
                    md_snap_slice = md_snapshot[md_snapshot.index == ts_index[i]].copy()
                    np_snap_slice = md_snap_slice[md_snap_slice['side'] == snap_side].values
 
                    col_map_snap = {md_snap_slice.columns[i]: i for i in range(len(md_snap_slice.columns))}
                    np_data[:, :, j] = 0
                    np_order_id[:, j] = 0
                    print('msn_to_ignore: {} - Snap shape: {}'.format(msn_to_ignore, np_snap_slice.shape[0]))

                    for k in range(len(np_snap_slice)):
                        pos = np_snap_slice[k, col_map_snap['position']] - 1
                        np_data[pos, 0, j] = np_snap_slice[k, col_map_snap['price']]
                        np_data[pos, 1, j] = np_snap_slice[k, col_map_snap['quantity']]
                        np_data[pos, 2, j] = np_snap_slice[k, col_map_snap['broker']]
                        np_order_id[pos, j] = np_snap_slice[k, col_map_snap['order_id']]

                # INSERT
                if np_incr[i, col_map['event_type']] == 'INSERT':
                    # Caso ordem não tenha sido inserida
                    if np_data[pos, 2, j] != 0:
                        np_data[pos+1:, :, j] = np_data[pos:-1, :, j]
                        np_order_id[pos+1:, j] = np_order_id[pos:-1, j]
                    np_data[pos, 0, j] = np_incr[i, col_map['price']]
                    np_data[pos, 1, j] = np_incr[i, col_map['quantity']]
                    np_data[pos, 2, j] = np_incr[i, col_map['broker']]
                    np_order_id[pos, j] = np_incr[i, col_map['order_id']]

                # DELETE
                if np_incr[i, col_map['event_type']] == 'DELETE':
                    np_data[pos:-1, :, j] = np_data[pos+1:, :, j]
                    np_order_id[pos:-1, j] = np_order_id[pos+1:, j]
                    np_data[-1, :, j] = np.zeros(3)
                    np_order_id[-1, j] = 0

                # CHANGE
                if np_incr[i, col_map['event_type']] == 'CHANGE':
                    np_data[pos, 0, j] = np_incr[i, col_map['price']]
                    np_data[pos, 1, j] = np_incr[i, col_map['quantity']]
                    np_data[pos, 2, j] = np_incr[i, col_map['broker']]
                    np_order_id[pos, j] = np_incr[i, col_map['order_id']]

                # DELETE_FROM
                if np_incr[i, col_map['event_type']] == 'DELETE_FROM':
                    a = np_data.shape[0]
                    b = pos + 1
                    c_ = a - b
                    np_data[:c_, :, j] = np_data[pos+1:, :, j]
                    np_data[c_:, :, j] = 0
                    np_order_id[:c_, j] = np_order_id[pos+1:, j]
                    np_order_id[c_:, j] = 0

                # DELETE_THRU
                if np_incr[i, col_map['event_type']] == 'DELETE_THRU':
                    np_data[:, :, j] = np.zeros((max_levels, 3))
                    np_order_id[:, j] = np.zeros((max_levels))

                order_book_index += [ts_index[i]]
                list_inc_code += [np_incr[i, col_map['inc_code']]]
                j += 1

                # Resetting numpy
                if j == split_size - 1 or i == len(np_incr) - 1:
                    last_data = np_data[:, :, j-1].copy()
                    last_order_id = np_order_id[:, j-1].copy()

                    price_data = pd.DataFrame(np_data[:, 0, :j].transpose(), index=order_book_index, columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                    quantity_data = pd.DataFrame(np_data[:, 1, :j].transpose(), index=order_book_index, columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                    broker_data = pd.DataFrame(np_data[:, 2, :j].transpose(), index=order_book_index, columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                    order_id_data = pd.DataFrame(np_order_id[:, :j].transpose(), index=order_book_index, columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                    inc_code_data = pd.DataFrame(list_inc_code, index=order_book_index)

                    price_data = pa.Table.from_pandas(price_data)
                    quantity_data = pa.Table.from_pandas(quantity_data)
                    broker_data = pa.Table.from_pandas(broker_data)
                    order_id_data = pa.Table.from_pandas(order_id_data)
                    inc_code_data = pa.Table.from_pandas(inc_code_data)

                    if c == 0:
                        price_writer = pq.ParquetWriter(price_file, price_data.schema)
                        quantity_writer = pq.ParquetWriter(quantity_file, quantity_data.schema)
                        broker_writer = pq.ParquetWriter(broker_file, broker_data.schema)
                        order_id_writer = pq.ParquetWriter(order_id_file, order_id_data.schema)
                        inc_code_writer = pq.ParquetWriter(inc_code_file, inc_code_data.schema)
                        c = 1

                    price_writer.write_table(price_data)
                    quantity_writer.write_table(quantity_data)
                    broker_writer.write_table(broker_data)
                    order_id_writer.write_table(order_id_data)
                    inc_code_writer.write_table(inc_code_data)

                    np_data = np.zeros((max_levels, 3, split_size), dtype=np.float32)
                    np_data[:, :, 0] = last_data.copy()
                    np_order_id = np.zeros((max_levels, split_size), dtype=np.float32)
                    np_order_id[:, 0] = last_order_id.copy()
                    list_inc_code = []
                    order_book_index = []
                    j = 0
            
            price_writer.close()
            quantity_writer.close()
            broker_writer.close()
            order_id_writer.close()
            inc_code_writer.close()
            c = 0
            del np_data, np_incr
            gc.collect()

    def create_level_book_files(self, symbol, date, max_levels=100):
        for side in ['bid', 'ask']:
            print('Working {} side of {} level book'.format(side, symbol))
            price_file = "{}-price-{}-{}.parquet".format(symbol, side, date)
            quantity_file = "{}-quantity-{}-{}.parquet".format(symbol, side, date)
            broker_file = "{}-broker-{}-{}.parquet".format(symbol, side, date)
            level_len_file = "{}-level_len-{}-{}.parquet".format(symbol, side, date)

            output_price_file = "/bigdata/level-book/" + price_file
            output_quantity_file = "/bigdata/level-book/" + quantity_file
            output_broker_file = "/bigdata/level-book/" + broker_file
            output_level_len_file = "/bigdata/level-book/" + level_len_file

            price_pf = pq.ParquetFile('/bigdata/order-book/' + price_file)
            quantity_pf = pq.ParquetFile('/bigdata/order-book/' + quantity_file)
            broker_pf = pq.ParquetFile('/bigdata/order-book/' + broker_file)
            num_rows = price_pf.metadata.num_row_groups

            c = 0
            for read_row in range(num_rows):
                price_data = price_pf.read_row_group(read_row).to_pandas()
                quantity_data = quantity_pf.read_row_group(read_row).to_pandas()
                broker_data = broker_pf.read_row_group(read_row).to_pandas()

                aggr_price = np.zeros(price_data.shape)
                aggr_quantity = np.zeros(quantity_data.shape)
                aggr_broker = np.zeros(broker_data.shape)
                aggr_level_len = np.zeros(price_data.shape)

                np_price = price_data.values
                np_quantity = quantity_data.values
                np_broker = broker_data.values
                ts_index = price_data.index

                #  ==================
                for row in range(aggr_price.shape[0]):
                    price_line = np_price[row, :]
                    quantity_line = np_quantity[row, :]
                    broker_line = np_broker[row, :]

                    i = np.nonzero(np.diff(price_line))[0] + 1
                    i = np.insert(i, 0, 0)

                    # Compute the result columns
                    c1 = price_line[i]
                    c2 = np.add.reduceat(quantity_line, i)
                    c3 = broker_line[i]
                    
                    uniques, idx, count = np.unique(price_line, return_counts=True, return_inverse=True)
                    c4 = count[idx][i]

                    # Concatenate the columns
                    aggr_price[row, :c1.shape[0]] = c1
                    aggr_quantity[row, :c2.shape[0]] = c2
                    aggr_broker[row, :c3.shape[0]] = c3
                    aggr_level_len[row, :c1.shape[0]] = c4

                df_price_data = pd.DataFrame(aggr_price[:, :max_levels], index=[ts_index], columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                df_quantity_data = pd.DataFrame(aggr_quantity[:, :max_levels], index=[ts_index], columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                df_broker_data = pd.DataFrame(aggr_broker[:, :max_levels], index=[ts_index], columns=['{}_{}'.format(side, i) for i in range(max_levels)])
                df_level_len_data = pd.DataFrame(aggr_level_len[:, :max_levels], index=[ts_index], columns=['{}_{}'.format(side, i) for i in range(max_levels)])

                df_price_data = pa.Table.from_pandas(df_price_data)
                df_quantity_data = pa.Table.from_pandas(df_quantity_data)
                df_broker_data = pa.Table.from_pandas(df_broker_data)
                df_level_len_data = pa.Table.from_pandas(df_level_len_data)

                if c == 0:
                    price_writer = pq.ParquetWriter(output_price_file, df_price_data.schema)
                    quantity_writer = pq.ParquetWriter(output_quantity_file, df_quantity_data.schema)
                    broker_writer = pq.ParquetWriter(output_broker_file, df_broker_data.schema)
                    level_len_writer = pq.ParquetWriter(output_level_len_file, df_level_len_data.schema)
                    c = 1

                price_writer.write_table(df_price_data)
                quantity_writer.write_table(df_quantity_data)
                broker_writer.write_table(df_broker_data)
                level_len_writer.write_table(df_level_len_data)
                del df_price_data, df_quantity_data, df_broker_data, df_level_len_data
                del aggr_price, aggr_quantity, aggr_broker, aggr_level_len
                gc.collect()

            price_writer.close()
            quantity_writer.close()
            broker_writer.close()
            broker_writer.close()
            level_len_writer.close()
    # Viewers
    def list_files(self):
        dict_files = {}
        dict_files['order-book'] = os.listdir('/bigdata/order-book')
        dict_files['level-book'] = os.listdir('/bigdata/level-book')
        dict_files['trades'] = os.listdir('/bigdata/trades')
        dict_files['events'] = os.listdir('/bigdata/events')
        return dict_files
 
    def list_db_tables(self):
        self._create_sql_connection()
        cursor = self.conn.cursor()
        cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        return [table[0] for table in cursor.fetchall()]
    
    def list_columns_in_table(self, table_name):
        self._create_sql_connection()
        tables = self.list_db_tables()
        if table_name in tables:
            cursor = self.conn.cursor()
            cursor.execute("Select * FROM {} LIMIT 0".format(table_name))
            return [column[0] for column in cursor.description]
        else:
            print(' [ PARQUET ] Table not found. Valid tables: ', ', '.join(tables))
            return []

    def get_distinct_item_from_table(self, item, table_name, where=None):
        self._create_sql_connection()
        tables = self.list_db_tables()
        if table_name in tables:
            columns = self.list_columns_in_table(table_name)
            if item in columns:
                cursor = self.conn.cursor()
                if where is None:
                    cursor.execute("SELECT DISTINCT {} FROM {}".format(item, table_name))
                    return [column[0] for column in cursor.fetchall()]
                else:
                    w = ' AND '.join(["{} '{}'".format(key, where[key]) for key in where])
                    cursor.execute("SELECT DISTINCT {} FROM {} WHERE {}".format(item, table_name, w))
                    return [column[0] for column in cursor.fetchall()]
            else:
                print(' [ PARQUET ] Item not found. Valid items: ', ', '.join(columns))
                return []
        else:
            print(' [ PARQUET ] Table not found. Valid tables: ', ', '.join(tables))
            return []

    def get_valid_dates(self, symbol, table):
        table_df = self.valid_dates.setdefault(table, {})
        if not symbol in table_df:
            dates = self.get_distinct_item_from_table('ts', table, {'symbol =' : symbol})#, 'ts >' : '2019-07-26 00:00:00'})
            dates = set([a.date() for a in dates])
            dates = [d.strftime('%Y-%m-%d') for d in dates]
            dates.sort()
            table_df[symbol] = dates
        return table_df[symbol]

    def get_created_dates_for_symbol(self, symbol, data_type):
        files = self.list_files()
        files_dates = []
        if data_type == 'trades':
            files = files.get(data_type, [])
            for file in files:
                fl = file.split('-')
                if fl[0] == symbol:
                    files_dates += ['-'.join([fl[1], fl[2], fl[3].split('.')[0]])]
        elif data_type == 'incremental' or data_type == 'snapshot':
            files = files.get('events', [])
            for file in files:
                fl = file.split('-')
                if fl[0] == symbol and fl[1] == data_type:
                    files_dates += ['-'.join([fl[2], fl[3], fl[4].split('.')[0]])]
        files_dates.sort()
        return files_dates

    def create_missing_inc(self, db = 'md_rt', initial_date=None, final_date=None, symbol=None, type_=None):
        A = self.consult.diff_events(initial_date=initial_date, final_date=final_date, symbol=symbol, type_=type_)
        for keys in A:
            for keys2 in A[keys]:
                if (bool(A[keys][keys2])) is True:
                    if keys2 == 'incremental':
                        for date in A[keys][keys2]:
                            self.create_book_events(keys, date, db=db)
                            
    def create_missing_trade(self, db ='md_rt', initial_date=None, final_date=None, symbol=None):
        A = self.consult.diff_trades(initial_date=initial_date, final_date=final_date, symbol=symbol)
        for keys in A:
            if bool(A[keys]) is True:
                for date in A[keys]:
                    self.create_trades_files(keys, date, db=db)

    def create_missing_mirror(self, db ='asimov_mirror', initial_date=None, final_date=None):
        A = self.consult.diff_mirror(initial_date=None, final_date=None)
        for date in A:
            self.create_mirror_files(date, db)

    def _hunts_daily_incremental(self, db ='asimov_mirror'):
        self._create_sql_connection(database=db)
        date_a = str(date.today())
        since = int(datetime.strptime(date_a, "%Y-%m-%d").strftime("%s")) * (1000 ** 3)
        until = (int(datetime.strptime(date_a, "%Y-%m-%d").strftime("%s")) + 24 * 60 * 60) * (1000 ** 3)
        string_mirror = "SELECT distinct symbol FROM {} WHERE ts between '{}' AND '{}'".format('order_mirror', since, until)
        self.mirror = pd.read_sql_query(string_mirror, self.conn)
        # wdo = parquet.mirror['symbol'][0]
        A = list(self.mirror['symbol'][0])

        if A[0] == 'W':
            wdo = self.mirror['symbol'][0]
            dol = "DOL" + A[3] + A[4] + A[5]

        elif A[0] == 'D':
            dol = self.mirror['symbol'][0]
            wdo = 'WDO' + A[3] + A[4] + A[5]

        return [dol, wdo]

    def create_daily_incremental(self, db ='md_rt'):
        symbols = self._hunts_daily_incremental()
        date_ = date.today().strftime("%Y-%m-%d")        
        for keys in symbols:
            self.create_book_events(keys, date_, db=db)
            self.create_trades_files(keys, date_, db=db)


           
if __name__ == '__main__':
    from asimov_database import ParquetCreator

    parquet = ParquetCreator()

    parquet._create_sql_connection('asimov_dropcopy')
    cursor = parquet.conn.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #     return [table[0] for table in cursor.fetchall()]

    def create(symbol, parq):
        parq.create_all_book_events_files_for_symbol(symbol, overwrite=False)
        parq.create_all_trade_files_for_symbol(symbol, overwrite=False)

    symbols = ['WDOH19',
            'DOLJ19', 'WDOJ19',
            'DOLK19', 'WDOK19',
            'DOLM19', 'WDOM19',
            'DOLN19', 'WDON19',
            'DOLQ19', 'WDOQ19',
            'DOLU19', 'WDOU19',
            'DOLV19', 'WDOV19',
            'DOLX19', 'WDOX19',
            'DOLZ19', 'WDOZ19']
    for symbol in symbols:
        create(symbol, parquet)
        # parquet.create_book_events(symbol, '2019-10-14')
        # parquet.create_trades_files(symbol, '2019-10-14')

    #parquet.create_missing_inc(symbol=['DOL', 'WDO'])

    #parquet.create_missing_inc(symbol=['DOL', 'WDO'])
    #parquet.create_missing_mirror()
