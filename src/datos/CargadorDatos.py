import numpy as np
import pandas as pd


class CargadorDatos:
    def __init__(self):
        self.__df = None
        self.__num_f = None
        self.__num_c = None
        self.__percent_null = None
        self.__ceros = None



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



    def csv_to_df(self, *pd_args, **pd_kwargs) -> dict:
        data = pd.read_csv(*pd_args, **pd_kwargs)
        self.__df = pd.DataFrame(data)
        return self.param_set()

    def param_set(self)-> dict:
        self.__num_f, self.__num_c = self.__df.shape
        cant_nulos = float(self.__df.isnull().sum().sum())
        porcentaje = (cant_nulos * 100) / self.__df.size
        self.__percent_null = porcentaje
        self.__ceros = int((self.__df == 0).sum().sum())
        return self.param_get()


    def param_get(self)-> dict:
        return {"Numero de filas": self.__num_f,
                "Numero de columnas": self.__num_c,
                "Porcentaje de nulos": self.__percent_null,
                "Ceros totales": self.__ceros,
                "Dataframe": self.__df}


    def save_df(self, nombre, *pd_args, **pd_kwargs) -> dict:
        self.__df.to_csv(f"../data/raw/{nombre}+.csv",
                         *pd_args, **pd_kwargs)