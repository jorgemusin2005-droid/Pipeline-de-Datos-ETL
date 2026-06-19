# Pipeline ETL Medallion con NYC Taxi

Proyecto de Data Engineering preparado para portfolio que implementa un
pipeline ETL completo sobre el dataset NYC TLC Yellow Taxi usando Apache Spark,
Python, MySQL, Docker Compose y Jupyter.

El proyecto sigue la Arquitectura Medallion:

- Bronze: archivos Parquet raw descargados desde NYC TLC.
- Silver: registros de viajes limpios, tipados y particionados.
- Gold: tablas dimensionales y analiticas listas para consumo.
- MySQL: capa relacional de serving con hechos, dimensiones y agregados.

## Arquitectura

```text
Datos publicos de NYC TLC
  |-- Yellow Taxi Trip Records Parquet
  |-- Taxi Zone Lookup CSV
        |
        v
data/bronze + data/raw/taxi_zones
        |
        v
ETL con PySpark
  |-- validacion de esquema
  |-- limpieza de nulos y outliers
  |-- casteo de tipos
  |-- normalizacion de fechas
  |-- enriquecimiento con zonas de taxi
        |
        v
Almacenamiento Medallion
  |-- data/silver/yellow_taxi_trips
  |-- data/gold/dim_date
  |-- data/gold/dim_zone
  |-- data/gold/fact_trips
  |-- data/gold/agg_daily_demand
  |-- data/gold/agg_zone_revenue
        |
        v
Data Warehouse en MySQL
        |
        v
Analisis en Jupyter Notebook
```

## Stack Tecnologico

- Python 3
- Apache Spark / PySpark
- MySQL 8.4
- Docker Compose
- JupyterLab
- pandas, SQLAlchemy, matplotlib, seaborn

## Estructura del Repositorio

```text
.
|-- data/
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|   `-- raw/taxi_zones/
|-- notebooks/
|-- sql/
|   `-- init.sql
|-- src/
|   |-- config/settings.py
|   |-- etl/extract.py
|   |-- etl/transform.py
|   |-- main.py
|   `-- utils/spark_utils.py
|-- tests/
|-- docker-compose.yml
`-- requirements.txt
```

## Requisitos Previos

- Docker y Docker Compose
- Python 3.10 o superior para ejecutar scripts auxiliares en local
- Espacio suficiente en disco para el archivo Parquet de muestra y las capas
  generadas

La muestra por defecto usa los viajes Yellow Taxi de enero de 2024 publicados en
la pagina oficial de NYC TLC Trip Record Data.

Fuente oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Ejecucion Paso a Paso

### 1. Levantar MySQL y Jupyter/PySpark

```bash
docker compose up -d --build
```

Servicios disponibles:

- MySQL: `localhost:3307`
- JupyterLab: `http://localhost:8888`
- Token de Jupyter: `nyc-taxi-etl`
- Spark UI durante la ejecucion: `http://localhost:4040`

### 2. Instalar dependencias minimas para descargar datos en local

```bash
python -m pip install requests tqdm
```

Este paso es suficiente para ejecutar `extract.py` desde tu maquina local. El
contenedor Docker ya incluye el runtime de Spark/Jupyter y las dependencias
principales para ejecutar el pipeline.

### 3. Descargar los datos fuente

```bash
python -m src.etl.extract
```

Este comando descarga:

- `data/bronze/yellow_tripdata_2024-01.parquet`
- `data/raw/taxi_zones/taxi_zone_lookup.csv`

Para sobrescribir archivos existentes:

```bash
python -m src.etl.extract --force
```

### 4. Ejecutar el pipeline ETL con Spark

Desde el contenedor Jupyter/PySpark:

```bash
docker exec -it nyc_taxi_pyspark_jupyter python -m src.main
```

El pipeline realiza:

- Limpieza Bronze -> Silver y escritura en Parquet particionado.
- Generacion de tablas Gold.
- Carga por JDBC en las tablas MySQL:
  - `dim_date`
  - `dim_zone`
  - `fact_trips`
  - `agg_daily_demand`
  - `agg_zone_revenue`

### 5. Abrir JupyterLab

Accede a:

```text
http://localhost:8888
```

Usa el token:

```text
nyc-taxi-etl
```

Crea `notebooks/01_data_analysis.ipynb` usando la plantilla de analisis del
proyecto.

## Conexion a MySQL

Credenciales por defecto para desarrollo local:

```text
Host: localhost
Port: 3307
Database: nyc_taxi_dw
User: etl_user
Password: etl_password
```

Dentro de Docker, el host de MySQL es `mysql` y el puerto interno sigue siendo
`3306`.

## Modelo de Datos

### Dimensiones

- `dim_date`: atributos de calendario para fechas de recogida y llegada.
- `dim_zone`: metadatos de las zonas de taxi de NYC.

### Tabla de Hechos

- `fact_trips`: tabla de hechos a nivel de viaje limpio.

### Agregados

- `agg_daily_demand`: metricas diarias de demanda e ingresos por zona de
  recogida.
- `agg_zone_revenue`: metricas de ingresos y propinas por par zona de
  recogida/llegada.

## Buenas Practicas Aplicadas

- Paquete Python modular bajo `src/`.
- Configuracion explicita en `src/config/settings.py`.
- Factory de SparkSession en `src/utils/spark_utils.py`.
- Logica de transformacion aislada en `src/etl/transform.py`.
- Logica de descarga aislada en `src/etl/extract.py`.
- Entorno Dockerizado con MySQL y PySpark/Jupyter.
- Validacion basica de entradas y manejo de excepciones.
- Codigo orientado a PEP8 con docstrings.

## Comandos Utiles

Ver contenedores en ejecucion:

```bash
docker compose ps
```

Ver logs del contenedor de Jupyter/PySpark:

```bash
docker logs nyc_taxi_pyspark_jupyter
```

Detener el stack:

```bash
docker compose down
```

Detener el stack y eliminar el volumen de MySQL:

```bash
docker compose down -v
```

Reconstruir la imagen de Jupyter/PySpark si cambian las dependencias:

```bash
docker compose build pyspark-jupyter
```

## Notas

El dataset de NYC TLC puede evolucionar con el tiempo. La capa de
transformacion maneja varias columnas opcionales de forma defensiva, pero los
cambios de esquema deben revisarse cuando se cambie de mes o de anio.
