import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine

def Create_API_Service(client_secret_file, api_service_name, api_version):
    client_secret_file =client_secret_file
    api_service_name =api_service_name
    api_version =api_version
    credentials = service_account.Credentials.from_service_account_file(client_secret_file)
    credentials.with_scopes(
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    return build(api_service_name,api_version, credentials=credentials)



def extract_Data_from_Sheets():
    CLIENT_SECRET_FILE = "path_of_client_secret_file_fromyour_pc"
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    gsheetId = 'sheet_id_of_your_google_sheet'

    service = Create_API_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION)

    response = service.spreadsheets().values().get(
        spreadsheetId=gsheetId,
        majorDimension='ROWS',
        range='Tabelle1!A:K'
    ).execute()
    response = response["values"]

    return response

def create_dataframe():

    data=extract_Data_from_Sheets()
    df = pd.DataFrame.from_dict(data)
    new_header = df.iloc[0]  # grab the first row for the header
    df = df[1:]  # take the data less the header row
    df.columns = new_header  # set the header row as the df header
    df.columns = df.columns.str.replace(' ', '')

    return df

def transform_data():
    df = create_dataframe()
    #remove euro sign from revenue and development
    df["Revenue"] = df["Revenue"].replace('\u20AC', '', regex=True).astype(float)
    pattern = {'\u20AC': '', '-': 0}
    df["DevelopmentCost"] = df["DevelopmentCost"].replace(pattern, regex=True).astype(float)
    # seperate the year from datum and make another column
    df['year'] = pd.DatetimeIndex(df['Datum']).year
    # development with accessories
    df['DevelopmentCost_with_accesories'] = df[df['withAccessories'] == "With accessories"]['DevelopmentCost']

    return df

def load_data_into_postgrsql():

    database = transform_data()
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/database_name', pool_pre_ping=True)
    engine.connect()
    database.to_sql("table", engine)


if __name__ == '__main__':
    load_data_into_postgrsql()