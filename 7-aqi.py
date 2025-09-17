import sqlite3
import geopandas
import polars
import pandas
from shapely import wkb

conn = sqlite3.connect("db.sqlite3")
cursor = conn.cursor()
cursor.execute('DELETE FROM aqi')

contaminants = polars.read_csv("data/aqi.csv")
contaminants = contaminants.select(
    contaminant = polars.col("contaminante").cast(polars.String),
    aqi = polars.col("registros_validados").cast(polars.Float32, strict=False),
    lat = polars.col("latitude").cast(polars.Float64),
    lon = polars.col("longitude").cast(polars.Float64),
    datetime = polars.col("fecha").cast(polars.Date)
).drop_nulls().to_pandas()

contaminants = geopandas.GeoDataFrame(contaminants[["contaminant", "aqi", "datetime"]], geometry=geopandas.points_from_xy(contaminants["lon"], contaminants["lat"], crs="EPSG:4326"))

# load communes
communes = cursor.execute("""
SELECT commune.id, commune.name, commune.geometry FROM commune
""").fetchall()
communes = pandas.DataFrame(communes, columns=["id", "name", "geometry"])
communes = communes.set_index("id")
communes["geometry"] = communes[["geometry"]].apply(wkb.loads) # type: ignore
communes = geopandas.GeoDataFrame(data=communes, geometry='geometry', crs="EPSG:4326")

# find the commune each contaminant was found at
contaminants = geopandas.sjoin(contaminants, communes, how='left')
contaminants["wkb"] = contaminants.geometry.to_wkb()

print(contaminants)

for row in contaminants.iterrows():
    contaminant = row[1]["contaminant"]
    aqi = row[1]["aqi"]
    datetime = int(row[1]["datetime"].timestamp())
    geometry = row[1]["wkb"]
    commune_id = row[1]["id"]

    cursor.execute("""
    INSERT INTO aqi (contaminant, aqi, datetime, geometry, commune_id) VALUES (?, ?, ?, ?, ?)
    """, (contaminant, aqi, datetime, geometry, commune_id))

conn.commit()