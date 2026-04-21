"""Base comun para cargar, resumir y persistir DataFrames del proyecto."""

import os
from pathlib import Path

import joblib
import pandas as pd

from datos.gestor_datos_aresep_clima import GestorDatos
from api.cliente_api_clima import ClienteAPI
from datos.GestorDBconn import GestorDBconn


class CargadorDatos:
    """Centraliza el DataFrame activo y las utilidades base de carga/guardado."""

    BASE_DIR = Path(__file__).resolve().parents[2]

    def __init__(self):
        # Estado compartido por las subclases que trabajan sobre el mismo DataFrame activo.
        self.__df = None
        self.__num_f = None
        self.__num_c = None
        self.__percent_null = None
        self.__ceros = None

        self.GestorDatos = GestorDatos
        self.ClienteAPI = ClienteAPI
        self.GestorDB = GestorDBconn()
        self.last_result = None

    @property
    def num_f(self):
        return self.__num_f

    @property
    def num_c(self):
        return self.__num_c

    @property
    def percent_null(self):
        return self.__percent_null

    @property
    def ceros(self):
        return self.__ceros

    @property
    def df(self):
        return self.__df

    @df.setter
    def df(self, df: pd.DataFrame):
        self.__df = df

    def _chain_response(self, resultado, chain):
        """Permite alternar entre retorno de datos y flujo encadenable."""
        # last_result conserva la ultima salida util cuando el metodo sigue encadenando.
        self.last_result = resultado
        if chain:
            return self
        return resultado

    def csv_to_df(self, *pd_args, chain=True, **pd_kwargs):
        """Carga un CSV en self.df y actualiza metricas basicas del dataset."""
        data = pd.read_csv(*pd_args, **pd_kwargs)
        self.__df = pd.DataFrame(data)
        return self._chain_response(self.param_set(), chain)

    def sql_table_to_df(self, nombre_tabla: str, schema="public", chain=True):
        """Trae una tabla SQL completa para reutilizarla como DataFrame."""
        query = f'SELECT * FROM "{schema}"."{nombre_tabla}";'
        self.__df = self.GestorDB.consultar(query)
        return self._chain_response(self.param_set(), chain)

    def sql_view_to_df(self, nombre_vista: str, schema="public", chain=True):
        """Trae una vista SQL completa para analisis o reprocesamiento."""
        query = f'SELECT * FROM "{schema}"."{nombre_vista}";'
        self.__df = self.GestorDB.consultar(query)
        return self._chain_response(self.param_set(), chain)

    def param_set(self) -> dict:
        # Resume el dataset para detectar rapido tamano, nulos y ceros despues de cada cambio.
        self.__num_f, self.__num_c = self.__df.shape
        cant_nulos = float(self.__df.isnull().sum().sum())
        porcentaje = (cant_nulos * 100) / self.__df.size
        self.__percent_null = porcentaje
        self.__ceros = int((self.__df == 0).sum().sum())
        return self.param_get()

    def param_get(self) -> dict:
        return {
            "Numero de filas": self.__num_f,
            "Numero de columnas": self.__num_c,
            "Porcentaje de nulos": self.__percent_null,
            "Ceros totales": self.__ceros,
            "Dataframe": self.__df
        }

    def save_df(self, nombre, *pd_args, chain=True, **pd_kwargs):
        """Exporta el DataFrame activo como CSV dentro de data/."""
        if "index" not in pd_kwargs:
            pd_kwargs["index"] = False
        ruta = self.BASE_DIR / "data" / f"{nombre}.csv"
        ruta.parent.mkdir(parents=True, exist_ok=True)
        self.__df.to_csv(ruta, *pd_args, **pd_kwargs)
        self.last_result = {"ok": True, "ruta": str(ruta)}
        if chain:
            return self
        return None

    def save_model(self, nombre, modelo=None, metadata=None, chain=True):
        """Guarda un modelo o payload de modelos dentro de src/modelos."""
        if modelo is None:
            modelo = getattr(self, "modelo", None)

        if modelo is None:
            raise ValueError("No hay modelo para guardar.")

        metadata = metadata or {}
        payload = {
            "modelo": modelo,
            **metadata,
        }

        nombre_archivo = nombre if str(nombre).endswith(".joblib") else f"{nombre}.joblib"
        ruta = self.BASE_DIR / "src" / "modelos" / nombre_archivo
        ruta.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(payload, ruta)

        resultado = {"ok": True, "ruta": str(ruta), "payload": payload}
        return self._chain_response(resultado, chain)

    def cargar_a_fact_dim(self, chain=True):
        """Dispara la carga del DW leyendo lo que ya existe en staging."""
        # La funcion SQL centraliza la logica de poblar dimensiones y hechos desde staging.
        resultado = self.GestorDB.ejecutar_funcion(
            "fn_cargar_a_fact_dim",
            schema="Fact_Dim",
            multiple_rows=True,
            commit=True
        )
        return self._chain_response(resultado, chain)

    def unificador_aresep_clima(self):
        """Flujo legado para generar clima crudo y unificar ARESEP + clima."""
        ruta_clima = self.BASE_DIR / "data" / "raw" / "api" / "clima_nasa_2018_2025.csv"

        empresas = [
            "CNFL",
            "COOPESANTOS",
            "COOPELESCA",
            "COOPEGUANACASTE",
            "COOPEALFARORUIZ",
            "ESPH",
            "JASEC",
            "ICE"
        ]

        if not ruta_clima.exists():
            print("No existe el archivo de clima. Descargando desde la API")

            cliente = self.ClienteAPI()
            df_clima = cliente.obtener_todas_empresas(empresas, "2018", "2025")

            print("\nDatos de clima descargados:")
            print(df_clima.head())
            print(df_clima.shape)

            cliente.guardar_csv(df_clima, str(ruta_clima))
            print(f"\nArchivo guardado en: {ruta_clima}")

        else:
            print(f"El archivo de clima ya existe: {ruta_clima}")
            print("No se volverÃ¡ a generar archivo desde la API.")

        print("\nProcesando datos...")

        gestor = self.GestorDatos()
        df_aresep, df_clima, df_final = gestor.procesar_todo()

        print("\nARESEP unificado:")
        print(df_aresep.head())
        print(df_aresep.shape)

        print("\nDataset final:")
        print(df_final.head())
        print(df_final.shape)
