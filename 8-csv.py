# export the data as CSVs

import pandas as pd
import sqlite3

sql = """
select
    report.description as reporte_descripcion,
    report.category as reporte_categoria,
    report.misc as reporte_extra,
    commune.name as comuna_nombre,
    commune.population as comunas_poblacion,
    commune.region as comuna_region,
    establishment.name as establecimiento_nombre,
    establishment.address as establecimiento_direccion,
    establishment.lat as establecimiento_latitud,
    establishment.lon as establecimiento_longitud,
    data.cohort as cohorte,
    data.year as año,
    data.value as valor
from data
join report on data.report_id = report.id
join establishment on data.establishment_id = establishment.id
join commune on data.commune_id = commune.id
"""

conn = sqlite3.connect("db.sqlite3")

data = pd.read_sql(sql, conn)
data.to_csv("data.csv", index=False)