"""Gestiona la conexion PostgreSQL y la ejecucion simple de consultas/funciones."""

import os

import pandas as pd

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None


class GestorDBconn:
    """Wrapper ligero para reutilizar una misma conexion durante la ETL."""

    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        # La configuracion prioriza argumentos y luego variables de entorno para no fijar credenciales.
        self.database = database or os.getenv("PGDATABASE", "DW_Energia_ML")
        self.user = user or os.getenv("PGUSER", "postgres")
        self.password = password or os.getenv("PGPASSWORD", "")
        self.host = host or os.getenv("PGHOST", "localhost")
        self.port = port or os.getenv("PGPORT", "5432")
        self.conn = None

    def conectar(self):
        """Abre la conexion solo cuando se necesita por primera vez."""
        if psycopg2 is None:
            raise ImportError(
                "No se encontro psycopg2. Instala el controlador para conectarte a PostgreSQL."
            )

        if self.conn is None or self.conn.closed != 0:
            # La conexion se crea una sola vez y luego se reutiliza durante la corrida.
            self.conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )

        return self.conn

    def cerrar(self):
        if self.conn is not None and self.conn.closed == 0:
            self.conn.close()
        self.conn = None

    def ejecutar(self, query, params=None, commit=False):
        """Ejecuta SQL sin retorno tabular y controla commit/rollback."""
        conn = self.conectar()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise

    def consultar(self, query, params=None):
        """Ejecuta un SELECT y devuelve el resultado como DataFrame."""
        conn = self.conectar()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            columnas = [desc[0] for desc in cursor.description]
            datos = cursor.fetchall()
        return pd.DataFrame(datos, columns=columnas)

    def ejecutar_funcion(self, nombre_funcion, params=None, schema="public", multiple_rows=False, commit=False):
        """Invoca funciones SQL del proyecto y adapta su salida a pandas/dict."""
        # Las funciones se consumen como SELECT para recuperar su confirmacion de carga.
        params = params or ()
        placeholders = ", ".join(["%s"] * len(params))
        query = f'SELECT * FROM "{schema}"."{nombre_funcion}"({placeholders});'
        resultado = self.consultar(query, params)

        if commit:
            self.conectar().commit()

        if multiple_rows:
            return resultado

        if resultado.empty:
            return {}

        return resultado.iloc[0].to_dict()

    def __enter__(self):
        self.conectar()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and self.conn is not None and self.conn.closed == 0:
            self.conn.rollback()
        self.cerrar()
