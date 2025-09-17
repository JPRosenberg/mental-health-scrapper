"""

imports communes into db

The geometry of the communes is serialized to well-known binary geometric representation (wkb) before inserted to the db

"""

import geopandas
import sqlite3
import pandas as pd
from lib.clean import clean_string
from unidecode import unidecode

geopandas.options.io_engine = "pyogrio"

communes = geopandas.read_file('data/communes/geometry/comunas.shp')
# geometry is in 3857
communes = communes.to_crs(4326)
communes["wkb"] = communes.geometry.to_wkb()
communes["Comuna"] = communes["Comuna"].apply(clean_string)
communes["Provincia"] = communes["Provincia"].apply(clean_string)
communes["Region"] = communes["Region"].apply(clean_string)

# save in sqlite database
conn = sqlite3.connect("db.sqlite3")
cursor = conn.cursor()

# wipe db
cursor.execute('DELETE FROM data')
cursor.execute('DELETE FROM establishment')
cursor.execute('DELETE FROM commune')

# insert
for comuna in communes.iterrows():
    name = clean_string(comuna[1]['Comuna'])
    region   = unidecode(comuna[1]['Region'])
    province = unidecode(comuna[1]['Provincia'])
    geometry = comuna[1]['wkb']

    cursor.execute('INSERT INTO commune (name, geometry, region, province, population) VALUES (?, ?, ?, ?, 0)', (name, geometry, region, province))

# format population csv
population = pd.read_csv("data/communes/population/pop.csv", sep=";")
population = pd.DataFrame(population.values[1:], columns=population.iloc[0]) # remove first row
population = population[population["NOMBRE COMUNA"] != 'PAÍS'] # remove country row
population = population[population.Edad == 'Total Comunal'] # only select total population
population = population[["NOMBRE COMUNA", "TOTAL"]]
population["TOTAL"] = population["TOTAL"].str.replace(".", "").astype(int)
population["NOMBRE COMUNA"] = population["NOMBRE COMUNA"]

# insert population
for pop in population.iterrows():
    name = clean_string(pop[1]['NOMBRE COMUNA'])
    population = pop[1]['TOTAL']
    cursor.execute('UPDATE commune SET population = ? WHERE name like ?', (population, name))

conn.commit()