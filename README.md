# Mental health exploration

This repository contains 3 different projects to scrape, organize and then explore a multitude of mental health reports from DEIS

All spatial data is stores as WKB and is in EPSG:4326

## Data sources
- communes: 
    - geometry: https://www.bcn.cl/siit/mapas_vectoriales (División comunal: polígonos de las comunas de Chile) (https://www.bcn.cl/obtienearchivo?id=repositorio/10221/10396/2/Comunas.zip) (download date: 2024-01-19)
    - population: https://es.wikipedia.org/wiki/Anexo:Comunas_de_Chile_por_población (http://www.censo2017.cl/wp-content/uploads/2017/12/Cantidad-de-Personas-por-Sexo-y-Edad.xlsx) (download date: 2024-02-18)
- establishments: https://deis.minsal.cl (DATOS ABIERTOS -> Establecimientos de Salud -> Listado de Establecimientos de Salud) (https://repositoriodeis.minsal.cl/DatosAbiertos/Establecimientos%20DEIS%20MINSAL%2002-02-2024.xlsx) (download date: 2024-02-18)
- reports: 0-scrapper, from two reports
    - reporteria adolescente: https://informesdeis.minsal.cl/SASVisualAnalytics/?reportUri=/reports/reports/0b3119f0-db06-4f10-a9cd-61092b5790bc&sectionIndex=12&sso_guest=true&reportViewOnly=true&reportContextBar=false&sas-welcome=false
    - reporteria salud mental: https://informesdeis.minsal.cl/SASVisualAnalytics/?reportUri=/reports/reports/ad0c03ad-ee7a-4da4-bcc7-73d6e12920cf&sso_guest=true&reportViewOnly=true&reportContextBar=false&sas-welcome=false
    
# Projects

This project consists of multiple scripts that scrape and process the data from DEIS

All the scripts must be run sequentially in the order they are numbered

## Running
From this folder, in a unix shell
```
# create a virtual environment
python3 -m venv venv 

# load the virtual environment
source venv/bin/activate

# install the requirements
pip install -r requirements.txt

# run the scripts sequentially
python 0-scrape.py
python 1-initdb.py
...
```
