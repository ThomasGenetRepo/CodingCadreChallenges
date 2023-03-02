import duckdb
import pandas as pd

db_loc = './challenge359.duckdb'

def main():
    db = duckdb.connect(db_loc)
    df = db.execute("SELECT * FROM challenge_solution").df()
    db.close()
    print(df.info())
    print(df.sample(50).sort_index())
    
if __name__=="__main__":
    main()