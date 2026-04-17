"""Extiende CargadorDatos con limpieza, validacion y exploracion rapida."""

import pandas as pd
import numpy as np

from src.datos.CargadorDatos import CargadorDatos


class ProcesadorEDA(CargadorDatos):
    """Aplica transformaciones sobre self.df sin salir del flujo ETL."""

    def __init__(self):
        super().__init__()

    def rm_col(self, *args, chain=True, **kwargs):
        """Elimina columnas o ejes completos y refresca metricas del dataset."""
        self.df = self.df.drop(*args, **kwargs)
        return self._chain_response(self.param_set(), chain)

    def rm_null(self, chain=True):
        self.df = self.df.dropna()
        return self._chain_response(self.param_set(), chain)

    def verif_consis(self, chain=False) -> tuple:
        """Reporta tipos base consistentes y filas que rompen el patron por columna."""
        clear = {}
        d_i = {}
        for nombre_columna in self.df.columns:
            col = list(self.df[nombre_columna])

            if not col:
                continue

            tipo_base = None
            posicion_base = None
            inconsistencias = []

            for j, valor in enumerate(col):
                if pd.isna(valor):
                    continue

                tipo_actual = type(valor)

                if tipo_base is None:
                    tipo_base = tipo_actual
                    posicion_base = j
                    continue

                if tipo_base != tipo_actual:
                    inconsistencias.append({
                        "columna": nombre_columna,
                        "Tipos": [tipo_base, tipo_actual],
                        "Pociciones": [posicion_base, j]
                    })

            if inconsistencias:
                d_i[nombre_columna] = inconsistencias
            elif tipo_base is not None:
                clear[nombre_columna] = tipo_base

        return self._chain_response((clear, d_i), chain)

    def col_uniques(self, columnas: list, chain=False) -> dict:
        d = {}
        for col in columnas:
            valores_unicos = self.df[col].unique().tolist()
            cantidad = self.df[col].nunique()
            d[col] = [cantidad, valores_unicos]
        return self._chain_response(d, chain)

    def col_nulls(self, reverse=False, chain=False) -> dict:
        d = {}
        for col in self.df.columns:
            datos_columna = self.df.loc[:, col]

            if isinstance(datos_columna, pd.DataFrame):
                cantidad = int(datos_columna.isnull().sum().sum())
            else:
                cantidad = int(datos_columna.isnull().sum())

            if reverse is False and cantidad > 0:
                d[col] = cantidad
            elif reverse is True and cantidad == 0:
                d[col] = cantidad
        return self._chain_response(d, chain)

    def convert(self, columnas: list, tipo: str, chain=True):
        """Convierte columnas al tipo solicitado usando pandas como capa comun."""
        for columna in columnas:
            tipo = tipo.lower()
            match tipo:
                case "int":
                    self.df[columna] = pd.to_numeric(self.df[columna], errors="coerce")
                case "float":
                    self.df[columna] = self.df[columna].astype(float)
                case "str":
                    self.df[columna] = self.df[columna].astype(str)
                case "bool":
                    self.df[columna] = self.df[columna].astype(bool)
                case "datetime":
                    self.df[columna] = pd.to_datetime(self.df[columna], errors="coerce")

        return self._chain_response(self.df, chain)

    def convert_lon_lat(self, col_lat=None, col_lon=None, chain=True):
        """Convierte coordenadas CRTM05 a grados geograficos dentro del DataFrame."""
        if col_lat is None and col_lon is None:
            return self._chain_response(self.df, chain)

        if col_lat is not None and col_lon is not None:
            x = pd.to_numeric(self.df[col_lon], errors="coerce")
            y = pd.to_numeric(self.df[col_lat], errors="coerce")
        elif col_lat is not None and "coordenadaX" in self.df.columns:
            x = pd.to_numeric(self.df["coordenadaX"], errors="coerce")
            y = pd.to_numeric(self.df[col_lat], errors="coerce")
        elif col_lon is not None and "coordenadaY" in self.df.columns:
            x = pd.to_numeric(self.df[col_lon], errors="coerce")
            y = pd.to_numeric(self.df["coordenadaY"], errors="coerce")
        else:
            raise ValueError("Se requieren columnas compatibles para convertir coordenadas.")

        # Constantes del sistema CRTM05 y del elipsoide de referencia.
        a = 6378137.0
        f = 1 / 298.257223563
        e2 = f * (2 - f)
        e4 = e2 ** 2
        e6 = e4 * e2
        e_prime2 = e2 / (1 - e2)
        k0 = 0.9999
        x0 = 500000.0
        lon0 = np.deg2rad(-84.0)

        # Convierte la coordenada norte proyectada en una latitud auxiliar.
        M = y / k0
        mu = M / (a * (1 - e2 / 4 - 3 * e4 / 64 - 5 * e6 / 256))

        # Coeficientes de la serie que aproximan la inversa de la proyeccion.
        e1 = (1 - np.sqrt(1 - e2)) / (1 + np.sqrt(1 - e2))
        j1 = 3 * e1 / 2 - 27 * (e1 ** 3) / 32
        j2 = 21 * (e1 ** 2) / 16 - 55 * (e1 ** 4) / 32
        j3 = 151 * (e1 ** 3) / 96
        j4 = 1097 * (e1 ** 4) / 512

        # Latitud preliminar usada como base para recuperar latitud/longitud reales.
        fp = (
            mu
            + j1 * np.sin(2 * mu)
            + j2 * np.sin(4 * mu)
            + j3 * np.sin(6 * mu)
            + j4 * np.sin(8 * mu)
        )

        # Valores auxiliares trigonometricos para la correccion final.
        sin_fp = np.sin(fp)
        cos_fp = np.cos(fp)
        tan_fp = np.tan(fp)

        c1 = e_prime2 * (cos_fp ** 2)
        t1 = tan_fp ** 2
        n1 = a / np.sqrt(1 - e2 * (sin_fp ** 2))
        r1 = (a * (1 - e2)) / np.power(1 - e2 * (sin_fp ** 2), 1.5)
        d = (x - x0) / (n1 * k0)

        # Recupera la latitud geografica en radianes.
        lat = fp - ((n1 * tan_fp) / r1) * (
            (d ** 2) / 2
            - ((5 + 3 * t1 + 10 * c1 - 4 * (c1 ** 2) - 9 * e_prime2) * (d ** 4)) / 24
            + ((61 + 90 * t1 + 298 * c1 + 45 * (t1 ** 2) - 252 * e_prime2 - 3 * (c1 ** 2)) * (d ** 6)) / 720
        )

        # Recupera la longitud geografica en radianes a partir del meridiano central.
        lon = lon0 + (
            d
            - ((1 + 2 * t1 + c1) * (d ** 3)) / 6
            + ((5 - 2 * c1 + 28 * t1 - 3 * (c1 ** 2) + 8 * e_prime2 + 24 * (t1 ** 2)) * (d ** 5)) / 120
        ) / cos_fp

        # Convierte el resultado final de radianes a grados decimales.
        if col_lat is not None:
            self.df[col_lat] = np.rad2deg(lat)

        if col_lon is not None:
            self.df[col_lon] = np.rad2deg(lon)

        return self._chain_response(self.df, chain)

    def col_names(self, chain=False) -> list:
        return self._chain_response(list(self.df.columns), chain)

    def matrix(self, chain=False):
        matrix = self.numeric_col(chain=False).corr()
        return self._chain_response(matrix, chain)

    def numeric_col(self, reverse=False, chain=False):
        if reverse is False:
            resultado = self.df.select_dtypes(include=[np.number]).copy()
        else:
            resultado = self.df.select_dtypes(exclude=[np.number]).copy()
        return self._chain_response(resultado, chain)

    def res_descrip(self, columnas: list, chain=False, *args, **kwargs):
        resultado = self.df[columnas].describe(*args, **kwargs)
        return self._chain_response(resultado, chain)

    def detect_outliers(self, columnas: list, chain=False) -> list:
        """Calcula outliers por IQR para columnas numericas seleccionadas."""
        l = []
        for columna in columnas:
            Q1 = self.df[columna].quantile(0.25)
            Q3 = self.df[columna].quantile(0.75)
            IQR = Q3 - Q1
            limite_inferior = Q1 - 1.5 * IQR
            limite_superior = Q3 + 1.5 * IQR
            outliers = self.df[(self.df[columna] < limite_inferior) | (self.df[columna] > limite_superior)]

            l.append({
                "columna": columna,
                "cantidad": len(outliers),
                "limite_inf": limite_inferior,
                "limite_sup": limite_superior,
                "datos": outliers[columna].tolist()
            })

        return self._chain_response(l, chain)

    def ceros_nan(self, to_cero=False, columnas=None, chain=True):
        """Intercambia ceros y NaN segun la etapa de limpieza que se necesite."""
        if columnas is None:
            columnas = list(self.df.columns)
        elif type(columnas) == str:
            columnas = [columnas]

        if to_cero is False:
            for col in columnas:
                self.df[col] = self.df[col].replace(0, np.nan)
        else:
            for col in columnas:
                self.df[col] = self.df[col].replace(np.nan, 0)

        return self._chain_response(self.param_set(), chain)

    def cant_ceros(self, x_col=False, chain=False):
        if x_col is True:
            resultado = (self.df == 0).sum()
        else:
            resultado = (self.df == 0).sum().sum()
        return self._chain_response(resultado, chain)

    def rangos(self, columna: str, v_min: int, v_max: int, df=None, chain=False):
        if df is None:
            resultado = self.df[(self.df[columna] <= v_min) | (self.df[columna] >= v_max)].copy()
        else:
            resultado = df[(df[columna] <= v_min) | (df[columna] >= v_max)].copy()
        return self._chain_response(resultado, chain)

    def filtrar_fecha(self, columna: str, año=None, mes=None, dia=None, chain=False):
        df = self.df.copy()
        if año:
            df = df[df[columna].dt.year == año]
        if mes:
            df = df[df[columna].dt.month == mes]
        if dia:
            df = df[df[columna].dt.day == dia]
        return self._chain_response(df, chain)

    def add_date_columns(self, date_col: str, chain=True):
        if date_col in self.df.columns:
            self.df[date_col] = pd.to_datetime(self.df[date_col], errors="coerce")
            self.df[f"{date_col}_year"] = self.df[date_col].dt.year
            self.df[f"{date_col}_month"] = self.df[date_col].dt.month
        return self._chain_response(self.df, chain)
