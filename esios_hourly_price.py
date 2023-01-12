import requests
import json
import pandas as pd
import datetime as dt
import sqlalchemy


def indicator_gather(base_url, token, indicator, start_date, end_date, geo_id):
    """
    Based on given parameters, fetches data using REE ESIOS API and returns dates and values of given indicator.

    :param base_url: curl from where data is gathered
    :type base_url: str
    :param token: esios ree api token
    :type token: str
    :param indicator: esios indicator for wanted data
    :type indicator: str
    :param start_date: starting date of wanted data
    :type start_date: datetime.date
    :param end_date: ending date of wanted data
    :param geo_id: id of corresponding country/region
    :type geo_id: int
    :return: pandas.DataFrame containing requested data for each datetime
    """

    headers = {"Accept": "application/json; application/vnd.esios-api-v1+json",
               "Content-Type": "application/json",
               "Host": "api.esios.ree.es",
               "Authorization": f"Token token={token}",
               "Cookie": ""}

    # converting datetime.date to str
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    # setting up parameters
    params = f"?start_date={start_date}T00%3A00%3A00Z&end_date={end_date}" \
             f"T00%3A00%3A00Z&time_trunc=hour&geo_ids[]={geo_id}"

    # fetching
    r = requests.get(base_url + indicator + params, headers=headers)

    print(r.status_code)

    # gathering data
    price_dict = {}  # empty dictionary to store data
    if r.status_code == 200:
        data = json.loads(r.text)
        print(data)
        index = 0  # index to store the data
        for el in data["indicator"]["values"]:
            price_dict[index] = [el["datetime"], el["datetime_utc"], el["value"]]
            index += 1

    # creating pandas DF from dictionary
    df = pd.DataFrame.from_dict(price_dict,
                                orient="index",
                                columns=["datetime", "datetime_utc", "value"])

    # replacing datetime column
    df["datetime"] = df["datetime"].apply(lambda x: pd.to_datetime(x, yearfirst=True).tz_convert('Europe/Amsterdam'))

    return df


def insert_into_db(df, table):
    """
    Creates db table if it doesn't exist and updates values from given df.

    :param df: dataframe with curated data to insert into specified table
    :type df: pandas.DataFrame
    """
    engine = sqlalchemy.create_engine('mysql+pymysql://root:DLC2022@127.0.0.1:3306/master_data_science')
    df_to_insert = df.copy()

    if table == "da_price":
        # creating table if doesn't exist:
        try:
            query = "CREATE TABLE IF NOT EXISTS da_price(" \
                    "datetime_utc DATETIME, " \
                    "sistema CHAR(2), " \
                    "fecha DATE NOT NULL, " \
                    "hora INT NOT NULL, " \
                    "bandera INTEGER NOT NULL, " \
                    "precio FLOAT NOT NULL, " \
                    "fecha_actualizacion DATETIME NOT NULL, " \
                    "PRIMARY KEY (datetime_utc, sistema))"
            engine.execute(query)
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            print(error)

    df_to_insert["fecha_actualizacion"] = dt.datetime.now()
    for i in range(len(df_to_insert)):
        try:
            df_to_insert.iloc[i:i + 1].to_sql(name=table, if_exists='append', con=engine, index=False)
        except:
            print("Duplicated key, skipping.")
            pass


if __name__ == "__main__":

    token = "28783fb5c499f634c81d9e1644cb7f46a05def938f799a054a62292ded53ee12"
    base_url = "https://api.esios.ree.es/indicators/"
    indicator = "600"
    start_date = dt.date.today() + dt.timedelta(days=-1)
    end_date = dt.date.today() + dt.timedelta(days=1)
    geo_id = 3
    # start_date = dt.date(2015, 1, 1)
    # end_date = start_date + dt.timedelta(days=30)
    # final_date = dt.date.today() + dt.timedelta(days=50)

    # while end_date <= final_date:
    df = indicator_gather(base_url, token, indicator, start_date, end_date, geo_id)

    # adding date and time columns
    df["fecha"] = pd.to_datetime(df["datetime"], utc=False)
    df["fecha"] = df["fecha"].dt.date
    df["hora"] = pd.to_datetime(df["datetime"], utc=False)
    df["hora"] = df["hora"].dt.time.apply(lambda x: x.hour + 1).astype(int)

    # adding system column
    df["sistema"] = "ES"

    # adding flag (summer = 1, winter = 0)
    df.loc[df["datetime"].dt.strftime('%d/%b/%Y:%H:%M:%S %z').str[-3:-2] == "1", "bandera"] = 0
    df.loc[df["datetime"].dt.strftime('%d/%b/%Y:%H:%M:%S %z').str[-3:-2] == "2", "bandera"] = 1

    # rearranging columns
    df = df[["datetime_utc", "sistema", "fecha", "hora", "bandera", "value"]]
    df.rename(columns={"value": "precio"}, inplace=True)

    # removing literals from UTC datetime
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], format="%Y-%m-%dT%H:%M:%S%fZ")

    # calling function to update db
    insert_into_db(df, "da_price")

    # start_date += dt.timedelta(days=31)
    # end_date = start_date + dt.timedelta(days=30)
