import sqlite3
import os

# connect to the database
os.unlink("db.sqlite3")

conn = sqlite3.connect("db.sqlite3")
cursor = conn.cursor()

# create the database
cursor.execute("""
CREATE TABLE commune (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    geometry BLOB NOT NULL,
    region TEXT NOT NULL,
    province TEXT NOT NULL,
    population INTEGER NOT NULL,
    income INTEGER
)
""")
cursor.execute("CREATE INDEX commune_province_index ON commune (province);")

cursor.execute("""
CREATE TABLE establishment (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    geometry BLOB NOT NULL,
    commune_id INTEGER NOT NULL,
    FOREIGN KEY (commune_id) REFERENCES commune (id)
)
""")
cursor.execute("CREATE INDEX establishment_commune_id_index ON establishment (commune_id);")

cursor.execute("""
CREATE TABLE report (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    category TEXT NOT NULL,
    misc TEXT
)
""")

cursor.execute("""
create table contaminant
(
    id INTEGER PRIMARY KEY,
    contaminant     TEXT not null,
    concentration   REAL not null,
    datetime       INTEGER not null,
    geometry        BLOB NOT NULL,
    commune_id      INTEGER not null,
    FOREIGN KEY (commune_id) REFERENCES commune (id)
)
""")
cursor.execute("CREATE INDEX contaminant_contaminant_index ON contaminant (contaminant);")
cursor.execute("CREATE INDEX contaminant_datetime_index ON contaminant (datetime);")

cursor.execute("""
create table data
(
    id INTEGER PRIMARY KEY,
    year             INTEGER    not null,
    cohort           TEXT       not null,
    value            INTEGER    not null,
    report_id        INTEGER    not null    references report,
    commune_id       INTEGER    not null    references commune,
    establishment_id INTEGER    not null    references establishment
)
""")
cursor.execute("CREATE INDEX data_report_id_index ON data (report_id);")
cursor.execute("CREATE INDEX data_year_commune_id_index ON data (year, commune_id);")
cursor.execute("CREATE INDEX data_year_index ON data (year);")
cursor.execute("CREATE INDEX data_establishment_id_index ON data (establishment_id);")
cursor.execute("CREATE INDEX data_commune_id_index ON data (commune_id);")

# commit the changes and close the connection
conn.commit()
conn.close()