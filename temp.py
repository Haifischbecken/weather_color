import pandas as pd
import requests
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
import shutil

BASEURL = "https://opendata.dwd.de/"
RELURL = "climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/"
FILENAME = "stundenwerte_TU_15000_akt"

print(f"{BASEURL}{RELURL}{FILENAME}.zip")
response = requests.get(f"{BASEURL}{RELURL}{FILENAME}.zip")

# with open(FILENAME, "wb") as outfile:
# outfile.write(response.content)

zf = zipfile.ZipFile(BytesIO(response.content))
print(zf.namelist())
try:
    shutil.rmtree("./wetter_daten")
except FileNotFoundError:
    pass
for member in zf.namelist():
    if member.startswith("produkt_tu_stunde"):
        zf.extract(member, path="wetter_daten")
        break

filelist = Path("./wetter_daten").rglob("*.txt")
for file in filelist:
    print(file)
df = pd.read_csv(file, sep=";")
reference_df = pd.read_csv("reference.txt", sep=";")
df = df.drop(["eor", "STATIONS_ID", "QN_9", "RF_TU"], axis=1)
reference_df = reference_df.drop(["eor", "STATIONS_ID", "QN_9", "RF_TU"], axis=1)
df["MESS_DATUM"] = pd.to_datetime(df["MESS_DATUM"], format="%Y%m%d%H")
reference_df["MESS_DATUM"] = pd.to_datetime(
    reference_df["MESS_DATUM"], format="%Y%m%d%H"
)
df = df.set_index("MESS_DATUM")
reference_df = reference_df.set_index("MESS_DATUM")

last_time = reference_df.index[-1]
reference_df = reference_df[datetime(2017, 1, 1) :]

reference_daily = reference_df.resample("D").max()
quantiles = reference_daily["TT_TU"].quantile(q=[i / 9 for i in range(1, 9)])


df = df.resample("D").max()


def quantilizer(quantiles, x):
    for i, q in enumerate(quantiles):
        if x < q:
            return i
    return len(quantiles)


# print(df)
df = df[datetime(2022, 1, 1) : datetime(2022, 12, 31)]
df["color"] = df["TT_TU"].apply(lambda x: quantilizer(quantiles, x))
print(quantiles)
print(df)
quantiles.to_csv("quantiles.csv", sep=";")
df.to_csv("result.csv", sep=";")