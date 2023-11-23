import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

# Define global variables
mountain_time_zone = pytz.timezone('US/Mountain')

# Global mapping for outcome types
OUTCOMES_MAP = {
    'Rto-Adopt': 1,
    'Adoption': 2,
    'Euthanasia': 3,
    'Transfer': 4,
    'Return to Owner': 5,
    'Died': 6,
    'Disposal': 7,
    'Missing': 8,
    'Relocate': 9,
    'N/A': 10,
    'Stolen': 11
}

def get_credentials():
    gcs_bucket_name = "datacenter_lab3"
    credentials_info = {
                            "type": "service_account",
                            "project_id": "poetic-standard-405721",
                            "private_key_id": "999ea8e4039edfa2f040a41352264c80bc19696e",
                            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCjEpIBuZck/8Nq\nlCT1Un9PEF1wKI3Ntsyve1bY1zV1mhHwgfOTuw5UCCMZZ0nE3Pe4Ca0wX+VbO31K\nS9E1UrEVotJKtTyT7XNaYFld3zWzJIOyoNz391Z+TFePcJXn0g/dLXIHLK0Gw9cG\nAZRSNO2qaogj3xh2ZtPxlP2nmcmQQT0fI7d2M5ox8+6AKDz26J4ZK4jiI5XA7J43\nTRj39Ul04Z8H1JYpHR4GCAkXiwREmaw9DM6RSTHb5cwqo9uX2JJRxh5nQ0DF2/I7\nLoVFnw507CfToRcbjTdGPjVX854JG38oEWutf7leOzPTBvidLcGd/YJkKatjpxrF\n8C0WXov1AgMBAAECggEARQmsIanSSNo0/66XVPrnpe/eNwkcO667ykET2/qEifS/\nyowhtHX5U28cePlG/F62S0ZheXzny2+MlAM0H3iSOwAzOmivLEtXTjhDLWA9yklX\nriy3UA15I0Arw7Dc7gd7Kt6+CzaJTDdmFYdepUz+H2s5lsIxB5NyADFdD/MmJ/kh\np8EY6PRhL1EhuMj2QfS0mP7NB2tyQ5CEzVWLKLObIMLduuZgCF2WHK+dSWK/FLmj\nbME2aBnadm/RLXohuelm4TCHF3gPzFjyEEeobhdBrD74zbcth3AUEnjdt6dTg3dD\nw5mL8X8dnCBFkLfFHeAoHJ8YM8gqNBYsDUEnCxcIMwKBgQDNGnS0cHczAqrBlp57\nTUG3mdlbxcE366uUKiA7kUNx0h9AcW24IiZnF6VaHqxgL9gEwvk6CLzBci6OK2YB\nvpk7klZQXJYbolhhFqP2n9JQ4Ckkz0ckLvdSDX+uVZgeShs9uApBzC12rkn+I2Vc\nBXyXY8gA5EYO+AozQbc4FJbSRwKBgQDLigj9QVaAelsr+ysupRb1WunNpjCu/b0B\nkHH7dtM+oCvUu7RZbFPP+YBoWc9X5XQicBBxDvFPRcLjzZlXXSpQJlyWbXvgZCnP\na8iH+qNDldqSXu9TyQP5hkugEZ0Hvspz1dB2a4cg7dhrnJpKtDysT/a13fFi+1DN\nE/fjMUmx4wKBgQDHDC/KlYL4/FOfQH9ZhmaKukJZcBfOH2cKib9yT822fodZ0Tr8\nAhhm4GnWhgIf7w6jwFyC18vnqNpJmxCKDOFFEQW3Q8Nh2jIPNxu00vIXDxSe5pJS\nKUpYVUVeeS8B5kV9pkg+BtrwXMDuZIePwLtjaLDHPMJI8Ktlhxc607BzLQKBgBLS\nFLq2S/VdWwjaFMgfY9wUKAJoF6BWvh9i5+dEuYvU67ikEq0iIy3b+E+t3kpWYUK9\n8gdCRnX6EWdXH5LAfqUipvUR8p5xJJyVbtmEB4y8UpWuSZsJv2BXVq7achbBQZ8s\najZJAxm8ZIKu3GWD/QhB7KIsf6GTc4lDC2zqpdHZAoGAMG+R6sF9rDH1WyWrqTUS\ngC+v8iwvonbpNFcpmvKc6LB2bug6HUTqsKiiwP3AkHUWRy8QnAHNoCakS2Vb9wiP\nKmb9JBLAXgRi94kdP1jkNhxuz1se7/YgPIAIiPxAOlfuT3PeIB2mbQT7HS/2QGRy\nCRZZ65HT0KFdr5KqWhJ3NzU=\n-----END PRIVATE KEY-----\n",
                            "client_email": "myserviceaccount@poetic-standard-405721.iam.gserviceaccount.com",
                            "client_id": "115136181489635961192",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/myserviceaccount%40poetic-standard-405721.iam.gserviceaccount.com",
                            "universe_domain": "googleapis.com"
                            }
    return credentials_info, gcs_bucket_name

def fetch_data_from_gcs(credentials_info, gcs_bucket_name):
    gcs_file_path = 'data/{}/outcomes_{}.csv'
    client = storage.Client.from_service_account_info(credentials_info)
    
    bucket = client.get_bucket(gcs_bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = gcs_file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    dataframe = pd.read_csv(StringIO(csv_data))

    return dataframe

def upload_to_gcs(dataframe, credentials_info, bucket_name, file_path):
    print(f"Uploading data to GCS at {file_path}.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished uploading data to GCS at {file_path}.")

def prepare_data(dataframe):
    dataframe['name'] = dataframe['name'].str.replace("*", "", regex=False)

    dataframe['sex'] = dataframe['sex_upon_outcome'].replace({"Neutered Male": "M",
                                                               "Intact Male": "M", 
                                                               "Intact Female": "F", 
                                                               "Spayed Female": "F", 
                                                               "Unknown": np.nan})

    dataframe['is_fixed'] = dataframe['sex_upon_outcome'].replace({"Neutered Male": True,
                                                                   "Intact Male": False, 
                                                                   "Intact Female": False, 
                                                                   "Spayed Female": True, 
                                                                   "Unknown": np.nan})

    dataframe['ts'] = pd.to_datetime(dataframe.datetime)
    dataframe['date_id'] = dataframe.ts.dt.strftime('%Y%m%d')
    dataframe['time'] = dataframe.ts.dt.time

    dataframe['outcome_type_id'] = dataframe['outcome_type'].replace(OUTCOMES_MAP)

    return dataframe

def prepare_animal_dimension(dataframe):
    print("Preparing Animal Dimensions Table Data")
    animal_dimension = dataframe[['animal_id','name','date_of_birth', 'sex', 'animal_type', 'breed', 'color']]
    animal_dimension.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']

    mode_sex = animal_dimension['sex'].mode().iloc[0]
    animal_dimension['sex'] = animal_dimension['sex'].fillna(mode_sex)
    
    return animal_dimension.drop_duplicates()

def prepare_date_dimension(dataframe):
    print("Preparing Date Dimension Table Data")
    date_dimension = pd.DataFrame({
        'date_id': dataframe.ts.dt.strftime('%Y%m%d'),
        'date': dataframe.ts.dt.date,
        'year': dataframe.ts.dt.year,
        'month': dataframe.ts.dt.month,
        'day': dataframe.ts.dt.day,
    })
    return date_dimension.drop_duplicates()

def prepare_outcome_types_dimension(dataframe):
    print("Preparing Outcome Types Dimension Table Data")
    outcome_types_dimension = pd.DataFrame.from_dict(OUTCOMES_MAP, orient='index').reset_index()
    outcome_types_dimension.columns = ['outcome_type', 'outcome_type_id']    
    return outcome_types_dimension

def prepare_outcomes_fact(dataframe):
    print("Preparing Outcome Fact Table Data")
    outcomes_fact = dataframe[["animal_id", 'date_id','time', 'outcome_type_id', 'is_fixed']]
    return outcomes_fact

def transform_data():
    credentials_info, gcs_bucket_name = get_credentials()
    new_data = fetch_data_from_gcs(credentials_info, gcs_bucket_name)
    new_data = prepare_data(new_data)

    dim_animal = prepare_animal_dimension(new_data)
    dim_dates = prepare_date_dimension(new_data)
    dim_outcome_types = prepare_outcome_types_dimension(new_data)

    fct_outcomes = prepare_outcomes_fact(new_data)

    upload_to_gcs(dim_animal, credentials_info, gcs_bucket_name, "transformed_data/dim_animal.csv")
    upload_to_gcs(dim_dates, credentials_info, gcs_bucket_name, "transformed_data/dim_dates.csv")
    upload_to_gcs(dim_outcome_types, credentials_info, gcs_bucket_name, "transformed_data/dim_outcome_types.csv")
    upload_to_gcs(fct_outcomes, credentials_info, gcs_bucket_name, "transformed_data/fct_outcomes.csv")

