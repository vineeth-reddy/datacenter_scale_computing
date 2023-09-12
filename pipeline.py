import pandas as pd
import argparse
import sys

def read_data(data):
    df= pd.read_csv(data)
    return df

def transform_data(data):
    df = data.copy()
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Month']= df['Order Date'].dt.month
    df['Year'] = df['Order Date'].dt.year 
    df['Day']= df['Order Date'].dt.day 
    df.drop(['Order Date'],axis=1,inplace=True)
    return df

def output_data(data):
    data.to_csv(args.target)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source',help='source csv')
    parser.add_argument('target',help='target csv')
    args = parser.parse_args()

    df= read_data(args.source)
    new_df = transform_data(df)
    output_data(new_df)





    

