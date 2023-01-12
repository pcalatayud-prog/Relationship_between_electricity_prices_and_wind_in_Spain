import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.sql import func


Base = automap_base()

# engine, suppose it has many tables
engine = create_engine('mysql+pymysql://root:DLC2022@127.0.0.1:3306/master_data_science')

# reflect the tables
Base.prepare(engine, reflect=True)

# mapped classes are now created with names by default
# matching that of the table name.
DaPrice = Base.classes.da_price
WindData = Base.classes.wind_data
session = Session(engine)

rset = (session.query(DaPrice.fecha.label("fecha"),
                      func.min(DaPrice.precio).label("precio_min"),
                      func.avg(DaPrice.precio).label("precio_avg"),
                      func.max(DaPrice.precio).label("precio_max"),
                      func.avg(WindData.vel_km_h).label("vel_avg"),
                      func.max(WindData.racha_max_km_h).label("racha_max")).
        filter(WindData.fecha == DaPrice.fecha)).group_by(DaPrice.fecha).all()

# print(list(rset))


"""SELECT dp.fecha, 
    MIN(dp.precio), 
    AVG(dp.precio), 
    MAX(dp.precio), 
    AVG(wd.vel_km_h), 
    MAX(wd.racha_max_km_h) 
FROM master_data_science.da_price dp
INNER JOIN master_data_science.wind_data wd ON wd.fecha = dp.fecha
GROUP BY dp.fecha;"""

fecha = [i[0] for i in rset]
min_precio = [i[1] for i in rset]
avg_precio = [i[2] for i in rset]
max_precio = [i[3] for i in rset]
avg_wind = [i[4] for i in rset]
max_wind = [i[5] for i in rset]

df = pd.DataFrame(
    {"fecha": fecha,
     "precio_min": min_precio,
     "precio_avg": avg_precio,
     "precio_max": max_precio,
     "vel_avg": avg_wind,
     "racha_max": max_wind,
    })

print(df.info())
print(df.corr())
df = df.loc[df["fecha"] <=dt.date(2020, 1, 1)]
df = df.loc[df["max_viento"] >= df["max_viento"].quantile(0.95)]

plt.scatter(df["min_precio"], df["max_viento"])
plt.show()
