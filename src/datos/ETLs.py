"""Carga DataFrames limpios a staging y catalogos del Data Warehouse."""

from pathlib import Path
from dataclasses import fields

import pandas as pd
from psycopg2.extras import execute_values

from src.datos.DataModels import (
    StgAresepMedios,
    StgCentro,
    StgClimaNasa,
    StgDistribucion,
    StgHidrocarburos,
)
from src.eda.ProcesadorEDA import ProcesadorEDA


class ETLs(ProcesadorEDA):
    """Usa self.df como fuente unica para poblar tablas de staging."""

    def __init__(self):
        super().__init__()
        self.batch_size = 500

    def _validate_df(self):
        """Evita ejecutar ETLs sin un DataFrame activo y valido."""
        if self.df is None or not isinstance(self.df, pd.DataFrame):
            raise ValueError("No hay un DataFrame cargado en la instancia para ejecutar la ETL.")

    def _ejecutar_etl_staging(self, nombre_funcion, modelo, tabla, chain):
        """Convierte filas a parametros SQL y las inserta por lotes en staging."""
        self._validate_df()

        # Cada fila se adapta al naming del staging usando su DataModel correspondiente.
        registros = [
            modelo.from_row(fila).to_params()
            for _, fila in self.df.iterrows()
        ]

        filas_exitosas = 0
        filas_error = 0
        errores = []
        columnas_sql = [campo.name for campo in fields(modelo)]
        columnas_insert = ", ".join(columnas_sql)
        query_lote = f'INSERT INTO "Staging".{tabla} ({columnas_insert}) VALUES %s'
        query_fila = (
            f'INSERT INTO "Staging".{tabla} ({columnas_insert}) '
            f'VALUES ({", ".join(["%s"] * len(columnas_sql))})'
        )

        conn = self.GestorDB.conectar()
        try:
            with conn.cursor() as cursor:
                for i in range(0, len(registros), self.batch_size):
                    # execute_values reduce el costo de enviar una fila por llamada al servidor.
                    lote = registros[i:i + self.batch_size]
                    execute_values(cursor, query_lote, lote, page_size=self.batch_size)
                    conn.commit()
                    filas_exitosas += len(lote)
                    print(
                        f"[STAGING] {tabla}: {filas_exitosas}/{len(registros)} filas cargadas"
                    )
        except Exception as batch_error:
            # Si falla el lote completo, se retrocede y se inspecciona fila por fila.
            conn.rollback()
            print(f"[ERROR][{tabla}] Falla en carga por lote: {batch_error}")
            errores.append({
                "tabla": tabla,
                "mensaje": f"Carga por lote fallida, cambiando a modo diagnostico: {batch_error}"
            })

            for params in registros:
                try:
                    self.GestorDB.ejecutar(query_fila, params=params, commit=True)
                    filas_exitosas += 1
                except Exception as row_error:
                    filas_error += 1
                    print(f"[ERROR][{tabla}] Fila fallida: {row_error}")
                    print(f"[ERROR][{tabla}] Parametros: {params}")
                    errores.append({
                        "tabla": tabla,
                        "mensaje": str(row_error),
                        "params": params
                    })

        resumen = {
            "tabla": tabla,
            "filas_totales": len(self.df),
            "filas_exitosas": filas_exitosas,
            "filas_con_error": filas_error,
            "errores": errores,
        }

        return self._chain_response(resumen, chain)

    def etl_stg_clima_nasa(self, chain=True):
        return self._ejecutar_etl_staging(
            "fn_stg_insert_clima_nasa",
            StgClimaNasa,
            "stg_clima_nasa",
            chain
        )

    def etl_stg_aresep_medios(self, chain=True):
        return self._ejecutar_etl_staging(
            "fn_stg_insert_aresep_medios",
            StgAresepMedios,
            "stg_aresep_medios",
            chain
        )

    def etl_stg_centro(self, chain=True):
        return self._ejecutar_etl_staging(
            "fn_stg_insert_centro",
            StgCentro,
            "stg_centro",
            chain
        )

    def etl_stg_distribucion(self, chain=True):
        return self._ejecutar_etl_staging(
            "fn_stg_insert_distribucion",
            StgDistribucion,
            "stg_distribucion",
            chain
        )

    def etl_stg_hidrocarburos(self, chain=True):
        return self._ejecutar_etl_staging(
            "fn_stg_insert_hidrocarburos",
            StgHidrocarburos,
            "stg_hidrocarburos",
            chain
        )

    def clear_staging(self, chain=True):
        """Vacía staging al inicio de una corrida completa del DW."""
        tablas = [
            "stg_hidrocarburos",
            "stg_distribucion",
            "stg_centro",
            "stg_aresep_medios",
            "stg_clima_nasa",
        ]

        for tabla in tablas:
            self.GestorDB.ejecutar(
                f'TRUNCATE TABLE "Staging".{tabla} RESTART IDENTITY;',
                commit=True
            )

        resumen = {
            "ok": True,
            "tablas_limpiadas": tablas
        }

        return self._chain_response(resumen, chain)

    def etl_catalogos(self, chain=True):
        """Carga los diccionarios CSV de data/docs_apis en el esquema Catalogo."""
        base_path = Path(__file__).resolve().parents[2] / "data" / "docs_apis"
        catalogos = {
            "aresep_centrales_electricas_variables.csv": (
                "catalogo_centrales_electricas_variables",
                [
                    "ID",
                    "Name",
                    "Tags",
                    "Services",
                    "Availability",
                    "Description",
                    "Keywords",
                    "Warnings_API_Original",
                ],
                [
                    "id",
                    "name",
                    "tags",
                    "services",
                    "availability",
                    "description",
                    "keywords",
                    "warnings_api_original",
                ],
            ),
            "aresep_hidrocarburos_variables.csv": (
                "catalogo_hidrocarburos_variables",
                [
                    "ID",
                    "Name",
                    "Tags",
                    "Services",
                    "Availability",
                    "Description",
                    "Keywords",
                    "Warnings_API_Original",
                ],
                [
                    "id",
                    "name",
                    "tags",
                    "services",
                    "availability",
                    "description",
                    "keywords",
                    "warnings_api_original",
                ],
            ),
            "aresep_tarifas_electricidad_distribucion_variables.csv": (
                "catalogo_tarifas_electricidad_distribucion_variables",
                [
                    "ID",
                    "Name",
                    "Tags",
                    "Services",
                    "Availability",
                    "Description",
                    "Keywords",
                    "Warnings_API_Original",
                ],
                [
                    "id",
                    "name",
                    "tags",
                    "services",
                    "availability",
                    "description",
                    "keywords",
                    "warnings_api_original",
                ],
            ),
            "aresep_tarifas_precios_medios_variables.csv": (
                "catalogo_tarifas_precios_medios_variables",
                [
                    "ID",
                    "Name",
                    "Tags",
                    "Services",
                    "Availability",
                    "Description",
                    "Keywords",
                    "Warnings_API_Original",
                ],
                [
                    "id",
                    "name",
                    "tags",
                    "services",
                    "availability",
                    "description",
                    "keywords",
                    "warnings_api_original",
                ],
            ),
            "nasa_power_parameters_name_es.csv": (
                "catalogo_clima_variables",
                [
                    "ID",
                    "Name_ES",
                    "Name_EN",
                    "Tags",
                    "Services",
                    "Availability",
                    "Description",
                    "Alternates",
                    "Keywords",
                    "Warnings",
                ],
                [
                    "id",
                    "name_es",
                    "name_en",
                    "tags",
                    "services",
                    "availability",
                    "description",
                    "alternates",
                    "keywords",
                    "warnings",
                ],
            ),
        }

        conn = self.GestorDB.conectar()
        resumen = []

        with conn.cursor() as cursor:
            for archivo, (tabla, columnas_csv, columnas_sql) in catalogos.items():
                # Los catalogos se reemplazan completos porque son diccionarios de referencia.
                ruta = base_path / archivo
                df_catalogo = pd.read_csv(ruta)
                df_catalogo = df_catalogo[columnas_csv].where(pd.notna(df_catalogo[columnas_csv]), None)
                registros = [tuple(fila) for fila in df_catalogo.itertuples(index=False, name=None)]

                cursor.execute(f'TRUNCATE TABLE "Catalogo".{tabla};')

                if registros:
                    columnas_insert = ", ".join(columnas_sql)
                    query = f'INSERT INTO "Catalogo".{tabla} ({columnas_insert}) VALUES %s'
                    execute_values(cursor, query, registros)

                resumen.append({
                    "tabla": tabla,
                    "archivo": archivo,
                    "filas_insertadas": len(registros)
                })

        conn.commit()
        return self._chain_response(resumen, chain)
