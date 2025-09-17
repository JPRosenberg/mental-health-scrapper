"""
Adds the establishments to the database
"""

import pandas as pd
import re
import sqlite3
from lib.clean import clean_string
import geopandas
from shapely import wkb

conn = sqlite3.connect("db.sqlite3")
cursor = conn.cursor()

# wipe db
cursor.execute('DELETE FROM data')
cursor.execute('DELETE FROM establishment')

# load the file with the addresses
addresses = pd.read_csv('data/establishments/establishments.csv', sep=';', index_col=0)
addresses = addresses[["Nombre Oficial", "Dirección", "Número", "Nombre Comuna", "Nombre Región", "LATITUD      [Grados decimales]", "LONGITUD [Grados decimales]"]]

# clean 
addresses["Nombre Oficial"] = addresses["Nombre Oficial"].apply(clean_string)
addresses["Nombre Comuna"] = addresses["Nombre Comuna"].apply(clean_string)

# replace commas with dots
addresses['lat'] = pd.to_numeric(addresses['LATITUD      [Grados decimales]'].apply(lambda x: re.sub(',', '.', x)), errors='coerce')
addresses['lon'] = pd.to_numeric(addresses['LONGITUD [Grados decimales]'].apply(lambda x: re.sub(',', '.', x)), errors='coerce')

addresses["geometry"] = geopandas.points_from_xy(addresses['lon'], addresses['lat'], crs="EPSG:4326")
addresses["geometry"] = addresses["geometry"].apply(wkb.dumps)

print(addresses)

# insert 
for address in addresses.iterrows():
    name = address[1]['Nombre Oficial']
    street = f"{address[1]['Dirección']}, {address[1]['Número']}, {address[1]['Nombre Región']}"

    geometry = address[1]["geometry"]

    comuna = address[1]['Nombre Comuna']
    print(comuna)
    cursor.execute('SELECT id FROM commune WHERE name LIKE ?', (comuna,))
    comuna_id = cursor.fetchone()
    comuna_id = comuna_id[0]

    cursor.execute("INSERT INTO establishment (name, address, geometry, commune_id) VALUES (?, ?, ?, ?)", (name, street, geometry, comuna_id))
    
conn.commit()