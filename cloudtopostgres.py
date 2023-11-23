import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine

class DataProcessor:

    def __init__(self):
        self.gcs_bucket_name = 'datacenter_lab3'
        self.db_configuration = {
            'dbname': 'shelterdb',
            'user': 'vineeth',
            'password': 'datacenter123',
            'host': '34.68.213.85',
            'port': '5432',
        }

    def retrieve_gcs_auth_info(self):
        return {
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

    def fetch_gcs_data(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'

        auth_info = self.retrieve_gcs_auth_info()
        client = storage.Client.from_service_account_info(auth_info)
        bucket = client.get_bucket(self.gcs_bucket_name)

        # Read the CSV file from GCS into a DataFrame
        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def establish_postgres_connection(self):
        connection = psycopg2.connect(**self.db_configuration)
        return connection

    def create_database_table(self, connection, table_name):
        cursor = connection.cursor()
        query = self.formulate_table_query(table_name)
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def formulate_table_query(self, table_name):
        if table_name == "dim_animals":
            return """CREATE TABLE IF NOT EXISTS dim_animals (
                            animal_id VARCHAR(7) PRIMARY KEY,
                            name VARCHAR,
                            dob DATE,
                            sex VARCHAR(1), 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR
                        );
                        """
        elif table_name == "dim_outcome_types":
            return """CREATE TABLE IF NOT EXISTS dim_outcome_types (
                            outcome_type_id INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
        elif table_name == "dim_dates":
            return """CREATE TABLE IF NOT EXISTS dim_dates (
                            date_id VARCHAR(8) PRIMARY KEY,
                            date DATE NOT NULL,
                            year INT2  NOT NULL,
                            month INT2  NOT NULL,
                            day INT2  NOT NULL
                        );
                        """
        else:
            return """CREATE TABLE IF NOT EXISTS fct_outcomes (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_id VARCHAR(7) NOT NULL,
                            date_id VARCHAR(8) NOT NULL,
                            time TIME NOT NULL,
                            outcome_type_id INT NOT NULL,
                            is_fixed BOOL,
                            FOREIGN KEY (animal_id) REFERENCES dim_animals(animal_id),
                            FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
                            FOREIGN KEY (outcome_type_id) REFERENCES dim_outcome_types(outcome_type_id)
                        );
                        """

    def load_data_into_postgres(self, connection, gcs_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_configuration['user']}:{self.db_configuration['password']}@{self.db_configuration['host']}:{self.db_configuration['port']}/{self.db_configuration['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcs_data)}")

def process_data_main(file_name, table_name):
    processor = DataProcessor()
    table_data_df = processor.fetch_gcs_data(file_name)

    postgres_processor = DataProcessor()
    table_query = postgres_processor.formulate_table_query(table_name)
    postgres_connection = postgres_processor.establish_postgres_connection()

    postgres_processor.create_database_table(postgres_connection, table_query)
    postgres_processor.load_data_into_postgres(postgres_connection, table_data_df, table_name)

