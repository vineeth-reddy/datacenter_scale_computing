import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

def retrieve_data_from_api(base_url, api_key, limit=50000, order='animal_id'):
    offset = 0
    all_data = []

    while True:
        params = {
            '$limit': str(limit),
            '$offset': str(offset),
            '$order': order,
            'apikey': api_key
        }

        response = requests.get(base_url, params=params)
        current_data = response.json()

        if not current_data:
            break

        all_data.extend(current_data)
        offset += limit

    return all_data

def extract_data_and_create_dataframe(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    data_list = [[entry.get(column, None) for column in columns] for entry in data]
    df = pd.DataFrame(data_list, columns=columns)
    return df

def upload_to_cloud_storage(dataframe, gcs_credentials_info, gcs_bucket_name, file_path):
    client = storage.Client.from_service_account_info(gcs_credentials_info)
    csv_data = dataframe.to_csv(index=False)

    bucket = client.get_bucket(gcs_bucket_name)

    current_date = datetime.now(pytz.timezone('US/Mountain')).strftime('%Y-%m-%d')
    formatted_file_path = file_path.format(current_date, current_date)

    blob = bucket.blob(formatted_file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to Cloud Storage with date: {current_date}.")

def main():
    base_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    api_key = '5g60pap5ab7fpp40p5copkmj1'
    gcs_credentials_info = {
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
    gcs_bucket_name = 'datacenter_lab3'
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    data_from_api = retrieve_data_from_api(base_url, api_key)
    shelter_data_frame = extract_data_and_create_dataframe(data_from_api)
    upload_to_cloud_storage(shelter_data_frame, gcs_credentials_info, gcs_bucket_name, gcs_file_path)


