# Prediccion Consumo Energia Costa Rica

Proyecto de integracion, limpieza y modelado de datos energeticos de Costa Rica.  
El repositorio concentra:

- extraccion desde APIs de NASA POWER y ARESEP
- limpieza exploratoria reproducida en notebooks
- carga a un Data Warehouse en PostgreSQL
- construccion de esquemas `Catalogo`, `Staging` y `Fact_Dim`
- vistas predictivas para precio y consumo electrico
- entrenamiento, comparacion y exportacion de predicciones con modelos de regresion

## Vision general

```mermaid
flowchart LR
    A["APIs externas"] --> B["Clientes API"]
    B --> C["DataFrames crudos"]
    C --> D["ProcesadorEDA / notebooks"]
    D --> E["DataFrames limpios"]
    E --> F["ETLs"]
    F --> G["Esquema Staging"]
    H["Catalogos CSV"] --> I["ETLs.etl_catalogos()"]
    I --> J["Esquema Catalogo"]
    G --> K["fn_cargar_a_fact_dim()"]
    K --> L["Esquema Fact_Dim"]
```

## Objetivo del proyecto

El flujo principal toma fuentes de tarifas electricas, clima, centrales electricas e hidrocarburos, las normaliza a un formato comun con pandas y luego las carga a PostgreSQL para construir un modelo dimensional listo para analisis posterior.

## Partes del proyecto

### 1. APIs y fuentes

- `src/api/cliente_api_clima.py`
  Consulta NASA POWER y genera el historico mensual de clima por empresa.

- `src/api/cliente_api_aresep.py`
  Consulta ARESEP y construye DataFrames para:
  - distribucion electrica
  - precios medios
  - centrales electricas
  - zonas de concesion por operador
  - hidrocarburos

### 2. Limpieza y unificacion

- `src/eda/ProcesadorEDA.py`
  Aplica conversiones, limpieza, validaciones, deteccion de nulos, outliers y transformaciones de coordenadas.

- `src/datos/gestor_datos_aresep_clima.py`
  Une los CSV historicos de ARESEP y el archivo de clima NASA para construir:
  - `aresep_unificado_2018_2025.csv`
  - `dataset_final_2018_2025.csv`

- `notebooks/`
  Contiene los flujos exploratorios usados para llegar a los CSV de referencia y validar la limpieza por dominio.

### 3. Carga al Data Warehouse

- `src/datos/CargadorDatos.py`
  Clase base que mantiene `self.df`, exporta CSV, consulta SQL y ejecuta la carga final del DW.

- `src/datos/ETLs.py`
  Carga DataFrames limpios al esquema `Staging` y los catalogos CSV al esquema `Catalogo`.

- `src/datos/DataModels/`
  Define los modelos de fila usados para traducir columnas de pandas al naming SQL del staging.

- `src/datos/GestorDBconn.py`
  Administra la conexion PostgreSQL y la ejecucion de SQL/funciones almacenadas.

### 4. Scripts SQL

- `src/base_datos/Esquema_Catalogos.sql`
  Crea las tablas de diccionario de variables.

- `src/base_datos/Esquema_Staging.sql`
  Crea las tablas temporales de carga.

- `src/base_datos/Esquema_Fact_Dim.sql`
  Crea dimensiones y hechos del DW.

- `src/base_datos/Funciones_Staging.sql`
  Define funciones de insercion por fila al staging.

- `src/base_datos/Funciones_Fact_Dim.sql`
  Define la funcion que puebla dimensiones y hechos desde staging.

- `src/base_datos/Vistas_Modelo_Predictivo.sql`
  Crea la vista `Fact_Dim.prediccion_precio_mes`, usada para predecir el precio promedio mensual de electricidad segun empresa, ubicacion, centrales, clima y periodo.

- `src/base_datos/Vistas_Tarifas_Electricidad_Empresa_Mes.sql`
  Crea la vista `Fact_Dim.prediccion_consumo_empresa`, usada para predecir consumo energetico mensual por empresa y tarifa electrica.

## Clases principales y como funcionan

### `CargadorDatos`

Clase base del flujo de datos.  
Responsabilidades:

- mantener el `DataFrame` activo en `self.df`
- cargar CSV o tablas SQL a pandas
- guardar DataFrames como CSV
- ejecutar `cargar_a_fact_dim()`
- soportar encadenamiento con `chain=True`

Uso conceptual:

```python
from src.datos.CargadorDatos import CargadorDatos

cargador = CargadorDatos()
cargador.csv_to_df("ruta.csv")
```

### `ProcesadorEDA`

Hereda de `CargadorDatos` y opera sobre el mismo `self.df`.  
Responsabilidades:

- eliminar columnas y nulos
- convertir tipos
- revisar consistencia
- detectar nulos, ceros y outliers
- transformar coordenadas proyectadas a longitud/latitud

Uso conceptual:

```python
from src.eda.ProcesadorEDA import ProcesadorEDA

eda = ProcesadorEDA()
eda.csv_to_df("archivo.csv").rm_col(["observaciones"]).ceros_nan(to_cero=True)
```

### `GestorDBconn`

Encapsula la conexion PostgreSQL.  
Responsabilidades:

- abrir la conexion bajo demanda
- ejecutar SQL directo
- consultar resultados como `DataFrame`
- invocar funciones SQL del proyecto

### `ETLs`

Hereda de `ProcesadorEDA`, por lo que puede limpiar y cargar usando la misma instancia.  
Responsabilidades:

- poblar `Staging` por dominio
- limpiar staging
- poblar `Catalogo` desde CSV

Cada metodo `etl_stg_*`:

1. valida que exista `self.df`
2. convierte cada fila a un `DataModel`
3. serializa los valores al orden del staging
4. inserta por lotes en PostgreSQL
5. si un lote falla, baja a modo diagnostico fila por fila


### `ModelosML`

Hereda de `ProcesadorEDA` y mantiene el mismo flujo basado en `self.df`.

Responsabilidades:

- cargar vistas SQL usando los metodos heredados de `CargadorDatos`
- separar columnas con texto y columnas numericas para revision EDA
- dividir datos en train, validacion y prueba
- entrenar varios algoritmos de regresion
- comparar algoritmos por porcentaje de acierto en validacion y prueba
- mantener en memoria el mejor modelo encontrado
- cargar modelos guardados desde `src/modelos`
- generar predicciones con datos ingresados manualmente

Algoritmos iniciales:

```python
[("RL", LinearRegression()), ("RF", RandomForestRegressor(random_state=42))]
```

El atributo `modelos` guarda los modelos entrenados por abreviacion:

```python
{
    "RL": LinearRegression(),
    "RF": RandomForestRegressor()
}
```

Despues de comparar, `modelo` conserva solo el mejor modelo en memoria, tambien con su abreviacion:

```python
{
    "RF": RandomForestRegressor()
}
```

El guardado final queda a criterio del usuario despues de revisar los porcentajes de precision.

### `DataModels`

Los archivos `StgClimaNasa.py`, `StgAresepMedios.py`, `StgCentro.py`, `StgDistribucion.py` y `StgHidrocarburos.py` representan una fila canonica de cada tabla staging.

La base comun es `_BaseStgModel.py`, que:

- traduce nombres de columnas usando `aliases`
- convierte `NaN` a `None`
- convierte `Timestamp` a `date`
- normaliza booleanos como `True`, `False` o `NULL`
- devuelve parametros en el orden correcto para SQL

## Flujo operativo desde la ingesta hasta el DW

### Flujo ETL principal

El script principal es:

- `src/ETL_main.py`

Secuencia resumida:

1. limpia staging
2. carga catalogos
3. extrae datos crudos desde ARESEP
4. construye datasets derivados de clima y medios
5. prepara y limpia cada dominio segun el flujo de notebooks
6. carga cada dominio a su tabla staging, incluyendo zonas
7. ejecuta `cargar_a_fact_dim()` para poblar dimensiones y hechos

```mermaid
flowchart TD
    A["ETL_main.py"] --> B["clear_staging()"]
    B --> C["etl_catalogos()"]
    C --> D["ClienteAPI ARESEP"]
    D --> E["DataFrames ARESEP crudos"]
    C --> F["GestorDatos.procesar_todo()"]
    E --> G["preparar_hidrocarburos()"]
    E --> H["preparar_centro()"]
    E --> I["preparar_zonas()"]
    E --> J["preparar_distribucion()"]
    F --> K["preparar_aresep_medios()"]
    F --> L["preparar_clima()"]
    G --> M["etl_stg_hidrocarburos()"]
    H --> N["etl_stg_centro()"]
    I --> O["etl_stg_zonas()"]
    J --> P["etl_stg_distribucion()"]
    K --> Q["etl_stg_aresep_medios()"]
    L --> R["etl_stg_clima_nasa()"]
    M --> S["cargar_a_fact_dim()"]
    N --> S
    O --> S
    P --> S
    Q --> S
    R --> S
    S --> T["Fact_Dim"]
```


## Flujo de entrenamiento predictivo

El notebook principal de entrenamiento es:

- `notebooks/Flujo_Modelo.ipynb`

Trabaja con dos vistas del esquema `Fact_Dim`:

- `prediccion_precio_mes`
- `prediccion_consumo_empresa`

### Modelo de precio

Objetivo:

```text
empresa_mes_tarifa_crc_promedio
```

El modelo predice el precio promedio mensual de electricidad usando la vista `prediccion_precio_mes`.

Para predicciones manuales se ingresan datos como:

```python
{
    "anio": 2026,
    "mes": 1,
    "trimestre": 1,
    "nombre_empresa": "ICE",
    "provincia": "CARTAGO",
    "canton": "CARTAGO",
    "distrito": "ORIENTAL",
    "coordenada_x": -83.9194,
    "coordenada_y": 9.8644,
}
```

Salida esperada:

```text
anio, mes, trimestre, nombre_empresa, provincia, canton, distrito,
coordenada_x, coordenada_y, empresa_mes_tarifa_crc_promedio
```

### Modelo de consumo

Objetivo:

```text
empresa_mes_tarifa_consumo_energia_kwh_total
```

El modelo predice el consumo energetico mensual por empresa y tarifa usando la vista `prediccion_consumo_empresa`.

Para predicciones manuales se ingresan datos como:

```python
{
    "anio": 2026,
    "mes": 1,
    "trimestre": 1,
    "nombre_empresa": "ICE",
    "tarifa_electricidad": "RESIDENCIAL",
}
```

Salida esperada:

```text
anio, mes, trimestre, nombre_empresa, tarifa_electricidad,
empresa_mes_tarifa_consumo_energia_kwh_total
```

### Evaluacion de modelos

El flujo entrena y compara los algoritmos `RL` y `RF`.

La comparacion muestra:

- acierto en validacion
- error en validacion
- acierto en prueba
- error en prueba
- `r2_validacion`
- `r2_prueba`

Ademas, el notebook muestra una tabla simple para el mejor modelo:

```text
real | prediccion | diferencia | error_% | acierto_%
```

### Exportacion de predicciones

El notebook genera predicciones para los proximos 12 meses y guarda un CSV por modelo en:

```text
data/predicciones/
```

Archivos generados:

```text
prediccion_precio_mes_RL_proximos_12_meses.csv
prediccion_precio_mes_RF_proximos_12_meses.csv
prediccion_consumo_empresa_RL_proximos_12_meses.csv
prediccion_consumo_empresa_RF_proximos_12_meses.csv
```

Cada CSV contiene los datos ingresados para la prediccion y la columna objetivo predicha. No incluye columnas auxiliares como `modelo` o `vista`.

### Flujo de limpieza reproducido desde notebooks

Los notebooks sirven como referencia funcional del tratamiento previo al staging:

- `EDA_HC.ipynb`
  elimina columnas auxiliares, convierte nulos numericos y deja el layout limpio de hidrocarburos

- `EDA_Dist.ipynb`
  identifica columnas utiles sin nulos y valida la estructura de distribucion

- `EDA_Centrales.ipynb`
  revisa el catalogo geografico y transforma coordenadas

- `EDA_Zonas.ipynb`
  separa el tipo de geometria desde la WKT y deja el layout final usado por `stg_zonas`

- `EDA_Medios.ipynb`
  revisa precios medios y su consistencia mensual

- `EDA_Clima.ipynb`
  valida las variables climaticas obtenidas desde NASA POWER

## Estructura de carpetas

```text
Prediccion_Consumo_Energia_Costa_Rica/
|-- README.md
|-- LICENSE
|-- data/
|   |-- docs_apis/
|   |   |-- aresep_*.csv
|   |   `-- nasa_power_parameters_name_es.csv
|   |-- predicciones/
|   |   |-- prediccion_consumo_empresa_RF_proximos_12_meses.csv
|   |   |-- prediccion_consumo_empresa_RL_proximos_12_meses.csv
|   |   |-- prediccion_precio_mes_RF_proximos_12_meses.csv
|   |   `-- prediccion_precio_mes_RL_proximos_12_meses.csv
|   |-- raw/
|   |   |-- api/
|   |   `-- aresep/
|   `-- processed/
|       |-- aresep_apis/
|       |-- aresep_unificado_2018_2025.csv
|       `-- dataset_final_2018_2025.csv
|-- notebooks/
|   |-- EDA_Centrales.ipynb
|   |-- EDA_Clima.ipynb
|   |-- EDA_Dist.ipynb
|   |-- EDA_HC.ipynb
|   |-- EDA_Medios.ipynb
|   |-- EDA_Zonas.ipynb
|   `-- Flujo_Modelo.ipynb
`-- src/
    |-- ETL_main.py
    |-- main.py
    |-- api/
    |   |-- cliente_api_aresep.py
    |   `-- cliente_api_clima.py
    |-- base_datos/
    |   |-- DW_ConsumoElect_Model.sql
    |   |-- Esquema_Catalogos.sql
    |   |-- Esquema_Staging.sql
    |   |-- Esquema_Fact_Dim.sql
    |   |-- Funciones_Staging.sql
    |   |-- Funciones_Fact_Dim.sql
    |   |-- Vistas_Modelo_Predictivo.sql
    |   `-- Vistas_Tarifas_Electricidad_Empresa_Mes.sql
    |-- datos/
    |   |-- CargadorDatos.py
    |   |-- ETLs.py
    |   |-- GestorDBconn.py
    |   |-- gestor_datos_aresep_clima.py
    |   `-- DataModels/
    |       |-- _BaseStgModel.py
    |       |-- StgClimaNasa.py
    |       |-- StgAresepMedios.py
    |       |-- StgCentro.py
    |       |-- StgDistribucion.py
    |       |-- StgHidrocarburos.py
    |       `-- StgZonasConcesion.py
    |-- eda/
    |   `-- ProcesadorEDA.py
    `-- modelos/
        `-- ModelosML.py
```

## Diagramas del Data Warehouse

### Esquema `Catalogo`

Los catalogos funcionan como diccionarios independientes de variables y parametros.

```mermaid
erDiagram
    catalogo_centrales_electricas_variables {
        string id
        string name
        text tags
        string services
        string availability
        text description
        text keywords
        text warnings_api_original
    }

    catalogo_hidrocarburos_variables {
        string id
        string name
        text tags
        string services
        string availability
        text description
        text keywords
        text warnings_api_original
    }

    catalogo_tarifas_electricidad_distribucion_variables {
        string id
        string name
        text tags
        string services
        string availability
        text description
        text keywords
        text warnings_api_original
    }

    catalogo_tarifas_precios_medios_variables {
        string id
        string name
        text tags
        string services
        string availability
        text description
        text keywords
        text warnings_api_original
    }

    catalogo_zonas_concesion_operador_variables {
        string id
        string name
        text tags
        string services
        string availability
        text description
        text keywords
        text warnings_api_original
    }

    catalogo_clima_variables {
        string id
        string name_es
        string name_en
        text tags
        string services
        string availability
        text description
        text alternates
        text keywords
        text warnings
    }
```

### Esquema `Staging`

El staging conserva la forma operativa de cada fuente antes de transformarla al modelo dimensional.

```mermaid
erDiagram
    stg_clima_nasa {
        string empresa
        int ano
        int mes
        float t2m
        float ws10m
        float cloud_amt
        float rh2m
        float t2m_max
        float t2m_min
        float cloud_od
        float gwetroot
        float ts
        float prectotcorr
        float allsky_sfc_sw_dwn
        float ps
        float t2mwet
        float allsky_sfc_sw_diff
        float allsky_sfc_lw_dwn
    }

    stg_aresep_medios {
        int mes
        int ano
        string empresa
        string tarifa
        float abonados
        float ventas
        float ingreso_sin_cvg
        float ingreso_con_cvg
        float precio_medio_sin_cvg
        float precio_medio_con_cvg
        string trimestre
        string sistema
        string trimestral
    }

    stg_centro {
        bigint stg_centro_key PK
        int id_objecto
        string operador
        string central_electrica
        string fuente
        string provincia
        string canton
        string distrito
        int codigo_dta
        float coordenada_x
        float coordenada_y
    }

    stg_zonas {
        bigint stg_zona_key PK
        int id_objecto
        string operador
        string descripcion
        float area
        string coordenadas
        string tipo_geometria
        int srid
        timestamp fecha_carga
    }

    stg_distribucion {
        bigint stg_distribucion_key PK
        int id_mes
        string mes
        int anho
        string empresa
        string tipo_tarifa
        string descripcion_tarifa
        string bloque
        float tarifa_promedio
    }

    stg_hidrocarburos {
        bigint stg_hidrocarburos_key PK
        string numero_expediente
        string numero_resolucion
        date fecha_publicacion
        string alcance_gaceta
        int numero_gaceta
        string producto
        float tipo_cambio
        float precio_referencia_internacional
        float precio_colonizado
        float otros_ingresos_prorrateados
        float asig_cruz_pesc
        float asig_cruz_minae
        float sub_cruz_pesc
        float sub_cruz_minae
        float diferencial_precios
        float impuesto_unico
        float canon
        float margen_operacion
        float rend_tarif
        float precio_plantel_sin_impuesto
        float precio_con_impuesto
        float margen_estaciones_terrestres
        float margen_estaciones_aereas
        float flete_estaciones
        float margen_envasador
        float margen_distribuidor
        float margen_detallista
        bool rige
        float precio_consumidor_final_gas
        float precio_final
        float precio_final_sin_punto_fijo
    }
```

### Esquema `Fact_Dim`

Este es el modelo dimensional final consumido por consultas y analisis.

```mermaid
erDiagram
    dim_tiempo {
        bigint tiempo_key PK
        date fecha
        int anio
        int mes
        int dia
        int trimestre
        string nombre_mes
        int anio_mes
    }

    dim_empresa {
        bigint empresa_key PK
        string nombre_empresa
    }

    dim_tarifa {
        bigint tarifa_key PK
        string tarifa_nk
        string nombre_tarifa
        string tipo_tarifa
        string descripcion_tarifa
        string bloque
        string sistema
        string indicador_trimestral
    }

    dim_ubicacion {
        bigint ubicacion_key PK
        string ubicacion_nk
        string provincia
        string canton
        string distrito
        int codigo_dta
        float coordenada_x
        float coordenada_y
    }

    dim_central_electrica {
        bigint central_key PK
        int id_objecto
        string operador
        string central_electrica
        string fuente
        bigint ubicacion_key FK
    }

    dim_producto_hidrocarburo {
        bigint producto_key PK
        string producto
    }

    dim_resolucion_hidrocarburo {
        bigint resolucion_key PK
        string resolucion_nk
        string numero_expediente
        string numero_resolucion
        date fecha_publicacion
        string alcance_gaceta
        int numero_gaceta
        bool rige
    }

    dim_zona_concesion {
        bigint zona_key PK
        int id_objecto
        string operador
        string descripcion
        float area
        string coordenadas_wkt
        string tipo_geometria
        int srid
    }

    bridge_empresa_zona {
        bigint bridge_empresa_zona_key PK
        bigint empresa_key FK
        bigint zona_key FK
        string fuente_vinculo
    }

    bridge_central_zona {
        bigint bridge_central_zona_key PK
        bigint central_key FK
        bigint zona_key FK
        string fuente_vinculo
    }

    fact_clima_mensual {
        bigint fact_clima_key PK
        bigint tiempo_key FK
        bigint empresa_key FK
        float t2m
        float ws10m
        float cloud_amt
        float rh2m
        float t2m_max
        float t2m_min
        float cloud_od
        float gwetroot
        float ts
        float prectotcorr
        float allsky_sfc_sw_dwn
        float ps
        float t2mwet
        float allsky_sfc_sw_diff
        float allsky_sfc_lw_dwn
    }

    fact_tarifa_electricidad {
        bigint fact_tarifa_key PK
        bigint tiempo_key FK
        bigint empresa_key FK
        bigint tarifa_key FK
        float abonados
        float ventas
        float ingreso_sin_cvg
        float ingreso_con_cvg
        float precio_medio_sin_cvg
        float precio_medio_con_cvg
    }

    fact_distribucion_tarifaria {
        bigint fact_distribucion_key PK
        bigint tiempo_key FK
        bigint empresa_key FK
        bigint tarifa_key FK
        float tarifa_promedio
    }

    fact_hidrocarburos {
        bigint fact_hidrocarburo_key PK
        bigint tiempo_key FK
        bigint producto_key FK
        bigint resolucion_key FK
        float tipo_cambio
        float precio_referencia_internacional
        float precio_colonizado
        float otros_ingresos_prorrateados
        float asig_cruz_pesc
        float asig_cruz_minae
        float sub_cruz_pesc
        float sub_cruz_minae
        float diferencial_precios
        float impuesto_unico
        float canon
        float margen_operacion
        float rend_tarif
        float precio_plantel_sin_impuesto
        float precio_con_impuesto
        float margen_estaciones_terrestres
        float margen_estaciones_aereas
        float flete_estaciones
        float margen_envasador
        float margen_distribuidor
        float margen_detallista
        float precio_consumidor_final_gas
        float precio_final
        float precio_final_sin_punto_fijo
    }

    dim_ubicacion ||--o{ dim_central_electrica : ubica
    dim_tiempo ||--o{ fact_clima_mensual : fecha
    dim_empresa ||--o{ fact_clima_mensual : empresa

    dim_tiempo ||--o{ fact_tarifa_electricidad : fecha
    dim_empresa ||--o{ fact_tarifa_electricidad : empresa
    dim_tarifa ||--o{ fact_tarifa_electricidad : tarifa

    dim_tiempo ||--o{ fact_distribucion_tarifaria : fecha
    dim_empresa ||--o{ fact_distribucion_tarifaria : empresa
    dim_tarifa ||--o{ fact_distribucion_tarifaria : tarifa

    dim_tiempo ||--o{ fact_hidrocarburos : fecha
    dim_producto_hidrocarburo ||--o{ fact_hidrocarburos : producto
    dim_resolucion_hidrocarburo ||--o{ fact_hidrocarburos : resolucion
    dim_empresa ||--o{ bridge_empresa_zona : empresa
    dim_zona_concesion ||--o{ bridge_empresa_zona : zona
    dim_central_electrica ||--o{ bridge_central_zona : central
    dim_zona_concesion ||--o{ bridge_central_zona : zona
```

## Scripts principales

### `src/ETL_main.py`

Script operativo principal.  
Recomendado para poblar:

- `Catalogo`
- `Staging`
- `Fact_Dim`

### `src/main.py`

Script de apoyo para generar y validar artefactos CSV locales del flujo clima + ARESEP.

## Variables de entorno para la BD

El proyecto esta pensado para no fijar credenciales en el codigo productivo.  
Las conexiones pueden resolverse con variables de entorno como:

- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`
- `PGHOST`
- `PGPORT`

## Orden recomendado de despliegue SQL

1. `Esquema_Catalogos.sql`
2. `Esquema_Staging.sql`
3. `Funciones_Staging.sql`
4. `Esquema_Fact_Dim.sql`
5. `Funciones_Fact_Dim.sql`

## Resumen

La arquitectura del proyecto va de la siguente manera:

- las APIs construyen DataFrames crudos
- `ProcesadorEDA` y los notebooks los limpian
- `ETLs` inserta catalogos y staging
- `CargadorDatos.cargar_a_fact_dim()` transforma staging en un modelo dimensional

Con esto se prepara el repositorio tanto para exploracion en notebooks como para una corrida ETL completa hacia PostgreSQL.
