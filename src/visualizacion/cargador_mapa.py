import pandas as pd
import geopandas as gpd
from shapely import wkt


class CargadorMapa:

    def __init__(self, ruta_csv):
        self.ruta_csv = ruta_csv
        self.df = None
        self.gdf = None

    def cargar_datos(self):
        self.df = pd.read_csv(self.ruta_csv, encoding="utf-8")
        return self.df

    def validar_columnas(self):
        columnas_requeridas = {"operador", "descripcion", "area", "coordenadas"}

        if self.df is None:
            raise ValueError("Primero debes cargar los datos.")

        faltantes = columnas_requeridas - set(self.df.columns)

        if faltantes:
            raise ValueError(f"Faltan columnas requeridas: {faltantes}")

    def convertir_a_geodf(self):
        if self.df is None:
            raise ValueError("Primero debes cargar los datos.")

        df_temp = self.df.copy()
        df_temp["geometry"] = df_temp["coordenadas"].apply(wkt.loads)

        self.gdf = gpd.GeoDataFrame(df_temp, geometry="geometry")

        # Si el mapa sale fuera de Costa Rica, esto se ajusta.
        self.gdf.set_crs(epsg=5367, inplace=True, allow_override=True)
        self.gdf = self.gdf.to_crs(epsg=4326)

        return self.gdf

    def obtener_geodatos(self):
        self.cargar_datos()
        self.validar_columnas()
        return self.convertir_a_geodf()