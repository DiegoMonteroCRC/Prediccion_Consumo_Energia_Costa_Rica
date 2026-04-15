import sys

sys.path.append("/Proyecto2/")
import pandas as pd
import numpy as np

from src.datos.CargadorDatos import CargadorDatos

class ProcesadorEDA(CargadorDatos):
    def __init__(self):
        super().__init__()



    def rm_col(self, *args, **kwargs):
        self.df = self.df.drop(*args, **kwargs)
        parms = self.param_set()
        return parms


    def rm_null(self):
        self.df = self.df.dropna()
        parms = self.param_set()
        return parms


    def verif_consis(self) -> tuple:
        clear = {}
        d_i = {}
        for nombre_columna in self.df.columns:
            col = self.df[nombre_columna].tolist()

            if not col:
                continue

            tipo_anterior = type(col[0])
            clear[nombre_columna] = tipo_anterior

            for j in range(1, len(col)):
                tipo_actual = type(col[j])

                if tipo_anterior != tipo_actual:
                    d_i.update({
                        "columna": nombre_columna,
                        "Tipos": [tipo_anterior, tipo_actual],
                        "Pociciones": [j - 1, j]
                    })
                    del clear[nombre_columna]
                    break

                tipo_anterior = tipo_actual


        return clear, d_i


    def col_uniques(self, columnas: list) -> dict:
        d = {}
        for col in columnas:
            valores_unicos = self.df[col].unique().tolist()
            cantidad = self.df[col].nunique()
            d[col] = [cantidad, valores_unicos]
        return d


    def convert(self,columnas:list, tipo:str):
        for columna in columnas:
            tipo = tipo.lower()
            match tipo:
                case "int":
                    self.df[columna] = pd.to_numeric(self.df[columna], errors='coerce')

                case "float":
                    self.df[columna] = self.df[columna].astype(float)

                case "str":
                    self.df[columna] = self.df[columna].astype(str)

                case "bool":
                    self.df[columna] = self.df[columna].astype(bool)

                case "datetime":
                    self.df[columna] = pd.to_datetime(self.df[columna])

        return self.df

    def col_names(self) -> list:
        return list(self.df.columns)

    def matrix(self):
        matrix = self.numeric_col().corr()
        return matrix

    def numeric_col(self):
        return self.df.select_dtypes(include=[np.number]).copy()

    def res_descrip(self, columnas:list, *args, **kwargs):
        return self.df[columnas].describe(*args, **kwargs)

    def detect_outliers(self, columnas: list) -> list:
        l = []
        for columna in columnas:
            Q1 = self.df[columna].quantile(0.25)
            Q3 = self.df[columna].quantile(0.75)

            IQR = Q3 - Q1

            limite_inferior = Q1 - 1.5 * IQR
            limite_superior = Q3 + 1.5 * IQR

            outliers = self.df[(self.df[columna] < limite_inferior) | (self.df[columna] > limite_superior)]

            l.append({
                "cantidad": len(outliers),
                "limite_inf": limite_inferior,
                "limite_sup": limite_superior,
                "datos": outliers[columna].tolist()
            })

        return l


    def ceros_nan(self, to_cero=False):
        if to_cero==False:
            for col in self.df.columns:
                    self.df[col] = self.df[col].replace(0, np.nan)
        else:
            for col in self.df.columns:
                self.df[col] = self.df[col].replace(np.nan, 0)

        return self.param_set()



    def cant_ceros(self, x_col= False):
        if x_col == True:
            return (self.df == 0).sum()
        else:
            return (self.df == 0).sum().sum()


    def rangos(self,columna:str, v_min:int, v_max:int, df=None):
        if df == None:
            return self.df[(self.df[columna] <= v_min) | (self.df[columna] >= v_max)].copy()
        else:
            return df[(df[columna] <= v_min) | (df[columna] >= v_max)].copy()


    def filtrar_fecha(self, columna: str, año=None, mes=None, dia=None):
        df = self.df.copy()
        if año:
            df = df[df[columna].dt.year == año]
        if mes:
            df = df[df[columna].dt.month == mes]
        if dia:
            df = df[df[columna].dt.day == dia]
        return df

    def add_date_columns(self, date_col: str):
        """Convierte una columna a datetime y añade año y mes."""
        if date_col in self.df.columns:
            self.df[date_col] = pd.to_datetime(self.df[date_col], errors='coerce')
            self.df[f"{date_col}_year"] = self.df[date_col].dt.year
            self.df[f"{date_col}_month"] = self.df[date_col].dt.month
        return self.df
