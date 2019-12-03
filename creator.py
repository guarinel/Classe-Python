from asimov_database import ParquetCreator, Consultor

class Creator:
    def __init__(self):
        self.parquet = ParquetCreator()
        self.consult = Consultor()

    def create_inc(self):
        A = self.consult.diff_events()
        for keys in A:
            for keys2 in A[keys]:
                if (bool(A[keys][keys2])) is True:
                    if keys2 == 'incremental':
                        for date in A[keys][keys2]:
                            self.parquet.create_book_events(keys, date)
                            
    def create_trade(self):
        A = self.consult.diff_trades()
        for keys in A:
            if bool(A[keys]) is True:
                for date in A[keys]:
                    self.parquet.create_trades_files(keys, date)

if __name__ == '__main__':
    from asimov_database import Creator
    create = Creator()
    create.create_trade()
    create.create_inc()
