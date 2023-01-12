import pandas as pd
import datetime as dt
import sqlalchemy
import os
import glob


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

    if table == "wind_data":
        # creating table if doesn't exist:
        try:
            query = "CREATE TABLE IF NOT EXISTS wind_data(" \
                    "fecha DATE, " \
                    "zona VARCHAR(20), " \
                    "vel_km_h FLOAT NOT NULL, " \
                    "vel_m_s FLOAT NOT NULL, " \
                    "vel_mph FLOAT NOT NULL, " \
                    "vel_nudos FLOAT NOT NULL, " \
                    "racha_max_km_h FLOAT NOT NULL, " \
                    "racha_max_m_s FLOAT NOT NULL, " \
                    "racha_max_mph FLOAT NOT NULL, " \
                    "racha_max_nudos FLOAT NOT NULL, " \
                    "hora_racha TIME, " \
                    "fecha_actualizacion DATETIME NOT NULL, " \
                    "PRIMARY KEY (fecha, zona))"
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


def format_hour(val):
    """
    Transforms hour in HH:MM:SS format to HH:MM, when it has wrong values like 82:10:00
    that should be 08:21

    :param val: value to be transformed
    :type val: str
    :return: transformed value (str)
    """
    if val != "nan" and val is not None:
        if len(val.split(":")) == 3:
            val_list = val.split(":")
            val = f"0{val_list[0][0]}:{val_list[0][-1]}{val_list[1][0]}"

    return val


if __name__ == "__main__":

    dir_path = r"...\wind_data"
    csv_files = glob.glob(os.path.join(dir_path, "*.csv"))

    dfl = []  # empty list to store all generated pandas DFs
    for file in csv_files:
        df = pd.read_csv(file, sep=";", parse_dates=["FECHA"])
        zona = (file.split("\\")[-1]).split("_")[0]
        df["Hora Racha"] = df["Hora Racha"].astype(str)
        df["zona"] = zona
        dfl.append(df)

    df = pd.concat(dfl)  # merging all dfs in the list into one

    # replacing "varios" for null
    df.replace({"Varias": None}, inplace=True)

    # renaming columns
    df.rename(columns={"FECHA": "fecha",
                       "Veloc. Media (Km/h)": "vel_km_h",
                       "Racha Max (Km/h)": "racha_max_km_h",
                       "Hora Racha": "hora_racha"},
              inplace=True)

    # fixing hour error
    df["hora_racha"] = df["hora_racha"].apply(lambda x: format_hour(x))

    # adding columns with values in m/s, mph and knots
    df["vel_m_s"] = df["vel_km_h"].apply(lambda x: x / 3.6)
    df["vel_mph"] = df["vel_km_h"].apply(lambda x: x / 0.44704)
    df["vel_nudos"] = df["vel_km_h"].apply(lambda x: x / 0.514444)
    df["racha_max_m_s"] = df["racha_max_km_h"].apply(lambda x: x / 3.6)
    df["racha_max_mph"] = df["racha_max_km_h"].apply(lambda x: x / 0.44704)
    df["racha_max_nudos"] = df["racha_max_km_h"].apply(lambda x: x / 0.514444)

    # rearranging columns before updating db
    cols = df.columns.tolist()
    cols = cols[:1] + cols[4:5] + cols[1:2] + cols[5:8] + cols[2:3] + cols[8:] + cols[3:4]
    df = df[cols]

    # calling function to update db
    insert_into_db(df, "wind_data")
