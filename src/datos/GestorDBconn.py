"""Gestiona la conexion PostgreSQL y la ejecucion simple de consultas/funciones."""

import os
from pathlib import Path

import pandas as pd

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None


class GestorDBconn:
    """Wrapper ligero para reutilizar una misma conexion durante la ETL."""

    def __init__(self, database=None, user=None, password=None, host=None, port=None):
        # La configuracion prioriza argumentos y luego variables de entorno para no fijar credenciales.
        self._database = database or os.getenv("PGDATABASE", "DW_Energia_ML")
        self._user = user or os.getenv("PGUSER", "postgres")
        self._password = password or os.getenv("PGPASSWORD", "")
        self._host = host or os.getenv("PGHOST", "localhost")
        self._port = port or os.getenv("PGPORT", "5432")
        self._conn = None

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, value):
        self._database = value

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        self._port = value

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, value):
        self._conn = value

    def _conectar(self):
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

    def _cerrar(self):
        if self.conn is not None and self.conn.closed == 0:
            self.conn.close()
        self.conn = None

    def _ejecutar(self, query, params=None, commit=False):
        """Ejecuta SQL sin retorno tabular y controla commit/rollback."""
        conn = self._conectar()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _consultar(self, query, params=None):
        """Ejecuta un SELECT y devuelve el resultado como DataFrame."""
        conn = self._conectar()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                columnas = [desc[0] for desc in cursor.description]
                datos = cursor.fetchall()
        except Exception:
            conn.rollback()
            raise
        return pd.DataFrame(datos, columns=columnas)

    def _ejecutar_funcion(self, nombre_funcion, params=None, schema="public", multiple_rows=False, commit=False):
        """Invoca funciones SQL del proyecto y adapta su salida a pandas/dict."""
        # Las funciones se consumen como SELECT para recuperar su confirmacion de carga.
        params = params or ()
        placeholders = ", ".join(["%s"] * len(params))
        query = f'SELECT * FROM "{schema}"."{nombre_funcion}"({placeholders});'
        conn = self._conectar()

        try:
            resultado = self._consultar(query, params)
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise

        if multiple_rows:
            return resultado

        if resultado.empty:
            return {}

        return resultado.iloc[0].to_dict()

    def _ejecutar_script_sql(self, ruta_script):
        """Ejecuta un archivo SQL completo dentro de la conexion activa."""
        conn = self._conectar()
        ruta_script = Path(ruta_script)
        try:
            script = ruta_script.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            script = ruta_script.read_text(encoding="cp1252")

        try:
            with conn.cursor() as cursor:
                cursor.execute(script)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def __enter__(self):
        self._conectar()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and self.conn is not None and self.conn.closed == 0:
            self.conn.rollback()
        self._cerrar()
