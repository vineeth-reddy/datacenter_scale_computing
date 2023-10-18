import pandas as pd
import sys
import argparse
from sqlalchemy import create_engine,text

def extract_data(input_file):
    return pd.read_csv(input_file)


def transform_data(data):
    transformed_data = data.copy()
    transformed_data[['mnth', 'yr']] = transformed_data['MonthYear'].str.split(' ', expand=True)
    transformed_data.drop(['MonthYear','Age upon Outcome'], axis=1, inplace = True)
    cols = {
    'Animal ID': 'animal_id',
    'Name': 'name',
    'DateTime': 'timestmp',
    'Date of Birth': 'dob',
    'Outcome Type': 'outcome_type',
    'Outcome Subtype': 'outcome_subtype',
    'Animal Type': 'animal_type',
    'Breed': 'breed',
    'Color': 'color',
    'Sex upon Outcome': 'sex'
    }
    transformed_data.rename(columns=cols, inplace=True)
    #store this into a temporary table for us to populate the fact table later
    return transformed_data


def load_data(transformed_data):
    db_dest = "postgresql+psycopg2://vineeth:data123$@db:5432/shelter"
    conn = create_engine(db_dest)
    time_df_data = transformed_data.copy()
    outcome_df_data = transformed_data.copy()
    transformed_data.to_sql("temp_table",conn,if_exists="append",index=False)
    time_df = time_df_data[['monthh','yearr']].drop_duplicates()
    time_df[['monthh','yearr']].to_sql("timingdim", conn, if_exists="append", index = False)
    transformed_data[['animal_id','animal_type','animal_name','dob','breed','color','sex','timestmp']].to_sql("animaldim", conn, if_exists="append", index = False)
    
    df = outcome_df_data[['outcome_type','outcome_subtype']].drop_duplicates()
    df[['outcome_type','outcome_subtype']].to_sql("outcomedim", conn, if_exists="append", index = False)
    sql_statement = text("""
    INSERT INTO outcomesfact (outcome_dim_key, animal_dim_key, time_dim_key)
    SELECT od.outcome_dim_key, a.animal_dim_key, td.time_dim_key
    FROM temp_table o
    JOIN outcomedim od ON o.outcome_type = od.outcome_type AND o.outcome_subtype = od.outcome_subtype
    JOIN timingdim td ON o.monthh = td.monthh AND o.yearr = td.yearr
    JOIN animaldim a ON a.animal_id = o.animal_id AND a.animal_type = o.animal_type AND a.timestmp = o.timestmp;
    """)
    with conn.begin() as connection:
        connection.execute(sql_statement)

if __name__ == "__main__":
    input_file = sys.argv[1]

    print("Start processing....")
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    print("Finished processing.....")
