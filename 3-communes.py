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

incomes = {
    "Santiago": 1001246,
    "Cerrillos": 822318,
    "Cerro Navia": 626151,
    "Conchalí": 720532,
    "El Bosque": 691029,
    "Estación Central": 740039,
    "Huechuraba": 1265786,
    "Independencia": 718622,
    "La Cisterna": 863400,
    "La Florida": 995239,
    "La Granja": 679636,
    "La Pintana": 595253,
    "La Reina": 1587617,
    "Las Condes": 1935473,
    "Lo Barnechea": 1781918,
    "Lo Espejo": 629630,
    "Lo Prado": 687574,
    "Macul": 1036119,
    "Maipú": 935903,
    "Ñuñoa": 1568029,
    "Pedro Aguirre Cerda": 707589,
    "Peñalolén": 1025659,
    "Providencia": 1797751,
    "Pudahuel": 844696,
    "Quilicura": 863021,
    "Quinta Normal": 754050,
    "Recoleta": 748137,
    "Renca": 709407,
    "San Joaquín": 772440,
    "San Miguel": 1158765,
    "San Ramón": 657161,
    "Vitacura": 2128023,
    "Puente Alto": 864959,
    "Pirque": 930518,
    "San José de Maipo": 952711,
    "Colina": 1281069,
    "Lampa": 961260,
    "Tiltil": 866047,
    "San Bernardo": 785232,
    "Buin": 889886,
    "Calera de Tango": 882261,
    "Paine": 694440,
    "Melipilla": 702238,
    "Alhué": 828121,
    "Curacaví": 799470,
    "María Pinto": 713639,
    "San Pedro": 697131,
    "Talagante": 869862,
    "El Monte": 719317,
    "Isla de Maipo": 803545,
    "Padre Hurtado": 873036,
    "Peñaflor": 879562
}

# clean incomes
incomes = {clean_string(k): v for k, v in incomes.items()}

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
    income = incomes.get(name, None)
    region   = unidecode(comuna[1]['Region'])
    province = unidecode(comuna[1]['Provincia'])
    geometry = comuna[1]['wkb']

    cursor.execute('INSERT INTO commune (name, geometry, region, province, population, income) VALUES (?, ?, ?, ?, 0, ?)', (name, geometry, region, province, income))

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