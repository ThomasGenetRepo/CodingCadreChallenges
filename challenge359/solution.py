# type: ignore
import os
import pandas as pd
import duckdb 
from dagster import asset, AssetIn

data_fpath = '../data/challenge359/raw_dates.csv'
db_path = './challenge359.duckdb'

@asset(group_name="Challenge359_ETL")
def extract():
    '''
    extract csv into a pandas df (which will be passed downstream in pipeline) and load/create into table in database
    '''
    df = pd.read_csv(data_fpath)
    db = duckdb.connect(db_path)
    db.execute("CREATE OR REPLACE TABLE dates AS SELECT * FROM df")
    
    return df

@asset(group_name="Challenge359_ETL")
def transform(extract):
    '''
    transforms to get the challenge solution;
    First split date to get month, and year. 
    Then attach a quarter to each month.
    Return a concacted column of Year+Quarter == solution!!! 
    '''
    col_to_transform = extract.columns[0]
    extract['months'] = extract[col_to_transform].str.split('/').map(lambda x: x[0])
    extract['year'] = extract[col_to_transform].str.split('/').map(lambda x: x[2])
    extract['quarters'] = extract['months'].map(
        lambda x: 'Q1' if x in ['1', '2', '3'] else (
                  'Q2' if x in ['4', '5', '6'] else (
                  'Q3' if x in ['7', '8', '9'] else 
                  'Q4'
            )
        )
    )
    challenge_answer = extract['year'] + extract['quarters']
    
    return pd.DataFrame({'OldDates': extract[col_to_transform], 'YearQuarter': challenge_answer})

@asset(group_name="Challenge359_ETL")
def load(transform):
    '''
    Complete the pipeline
    '''
    db = duckdb.connect(db_path)
    db.execute("CREATE OR REPLACE TABLE challenge_solution AS SELECT * FROM transform")
    
    return
    
