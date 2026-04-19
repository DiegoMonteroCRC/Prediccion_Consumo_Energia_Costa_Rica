"""Cliente NASA POWER para construir el histórico mensual de clima por empresa."""

from __future__ import annotations

import os
from typing import Dict, List

import pandas as pd
import requests


class ClienteAPI:
    """Consulta NASA POWER usando coordenadas tomadas desde Centro.csv."""

    def __init__(self, ruta_centro: str | None = None):
        self.url = "https://power.larc.nasa.gov/api/temporal/monthly/point"
        self.ruta_centro = ruta_centro or "../data/processed/aresep_apis/Centro.csv"
        self.parametros = (
            "T2M,WS10M,CLOUD_AMT,RH2M,T2M_MAX,T2M_MIN,CLOUD_OD,GWETROOT,TS,"
            "PRECTOTCORR,ALLSKY_SFC_SW_DWN,PS,T2MWET,ALLSKY_SFC_SW_DIFF,ALLSKY_SFC_LW_DWN"
        )
        self.mapa_operadores = {
            "COMPAÑIA NACIONAL DE FUERZA Y LUZ S.A.": "CNFL",
            "COOPERATIVA DE ELECTRIFICACION RURAL LOS SANTOS R.L.": "COOPESANTOS",
            "COOPERATIVA DE ELECTRIFICACION RURAL DE SAN CARLOS R.L.": "COOPELESCA",
            "COOPERATIVA DE ELECTRIFICACION RURAL DE GUANACASTE R.L.": "COOPEGUANACASTE",
            "COOPERATIVA DE ELECTRIFICACIÓN RURAL DE ALFARO RUIZ R.L.": "COOPEALFARORUIZ",
            "COOPERATIVA DE ELECTRIFICACION RURAL DE ALFARO RUIZ R.L.": "COOPEALFARORUIZ",
            "EMPRESA DE SERVICIOS PUBLICOS DE HEREDIA S.A.": "ESPH",
            "JUNTA ADMINISTRATIVA DEL SERVICIO ELECTRICO MUNICIPAL DE CARTAGO": "JASEC",
            "INSTITUTO COSTARRICENSE DE ELECTRICIDAD": "ICE",
        }
        self.coordenadas = self._cargar_coordenadas_desde_centro()

    def _cargar_coordenadas_desde_centro(self) -> Dict[str, List[dict]]:
        if not os.path.exists(self.ruta_centro):
            raise FileNotFoundError(
                f"No se encontró Centro.csv en la ruta esperada: {self.ruta_centro}"
            )

        df = pd.read_csv(self.ruta_centro, encoding="utf-8")
        df.columns = df.columns.str.strip()

        columnas_requeridas = {"operador", "coordenadaX", "coordenadaY"}
        faltantes = columnas_requeridas.difference(df.columns)
        if faltantes:
            raise KeyError(f"Centro.csv no contiene las columnas requeridas: {sorted(faltantes)}")

        df["operador"] = df["operador"].astype(str).str.strip()
        df["Empresa"] = df["operador"].map(self.mapa_operadores)
        df["coordenadaX"] = pd.to_numeric(df["coordenadaX"], errors="coerce")
        df["coordenadaY"] = pd.to_numeric(df["coordenadaY"], errors="coerce")

        df = df.dropna(subset=["Empresa", "coordenadaX", "coordenadaY"])
        df = df.drop_duplicates(subset=["Empresa", "coordenadaX", "coordenadaY"])

        coordenadas: Dict[str, List[dict]] = {}
        for empresa, grupo in df.groupby("Empresa"):
            coordenadas[empresa] = [
                {"lat": fila["coordenadaY"], "lon": fila["coordenadaX"]}
                for _, fila in grupo.iterrows()
            ]

        if not coordenadas:
            raise ValueError("No se pudieron construir coordenadas válidas desde Centro.csv")

        return coordenadas

    def obtener_empresas_disponibles(self) -> List[str]:
        return sorted(self.coordenadas.keys())

    def obtener_datos_empresa(self, empresa: str, inicio: str, fin: str) -> pd.DataFrame:
        empresa = empresa.strip().upper()
        if empresa not in self.coordenadas:
            raise KeyError(f"No hay coordenadas configuradas para la empresa: {empresa}")

        lista_df = []
        for punto in self.coordenadas[empresa]:
            params = {
                "parameters": self.parametros,
                "community": "RE",
                "longitude": punto["lon"],
                "latitude": punto["lat"],
                "start": inicio,
                "end": fin,
                "format": "json",
            }

            respuesta = requests.get(self.url, params=params, timeout=30)
            respuesta.raise_for_status()
            datos = respuesta.json()
            parametros = datos["properties"]["parameter"]
            fechas = list(parametros["T2M"].keys())

            df = pd.DataFrame({
                "fecha": fechas,
                "T2M": list(parametros["T2M"].values()),
                "WS10M": list(parametros["WS10M"].values()),
                "CLOUD_AMT": list(parametros["CLOUD_AMT"].values()),
                "RH2M": list(parametros["RH2M"].values()),
                "T2M_MAX": list(parametros["T2M_MAX"].values()),
                "T2M_MIN": list(parametros["T2M_MIN"].values()),
                "CLOUD_OD": list(parametros["CLOUD_OD"].values()),
                "GWETROOT": list(parametros["GWETROOT"].values()),
                "TS": list(parametros["TS"].values()),
                "PRECTOTCORR": list(parametros["PRECTOTCORR"].values()),
                "ALLSKY_SFC_SW_DWN": list(parametros["ALLSKY_SFC_SW_DWN"].values()),
                "PS": list(parametros["PS"].values()),
                "T2MWET": list(parametros["T2MWET"].values()),
                "ALLSKY_SFC_SW_DIFF": list(parametros["ALLSKY_SFC_SW_DIFF"].values()),
                "ALLSKY_SFC_LW_DWN": list(parametros["ALLSKY_SFC_LW_DWN"].values()),
            })

            df["fecha"] = df["fecha"].astype(str)
            df = df[df["fecha"].str[-2:] != "13"]
            df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m")
            df["Año"] = df["fecha"].dt.year
            df["Mes"] = df["fecha"].dt.month
            df["Empresa"] = empresa
            lista_df.append(df)

        df_final = pd.concat(lista_df, ignore_index=True)
        columnas_clima = [
            "T2M", "WS10M", "CLOUD_AMT", "RH2M", "T2M_MAX", "T2M_MIN",
            "CLOUD_OD", "GWETROOT", "TS", "PRECTOTCORR", "ALLSKY_SFC_SW_DWN",
            "PS", "T2MWET", "ALLSKY_SFC_SW_DIFF", "ALLSKY_SFC_LW_DWN",
        ]
        return df_final.groupby(["Empresa", "Año", "Mes"], as_index=False)[columnas_clima].mean()

    def obtener_todas_empresas(self, empresas: List[str], inicio: str, fin: str) -> pd.DataFrame:
        lista_empresas = []
        total = len(empresas)

        for i, empresa in enumerate(empresas, start=1):
            print(f"[{i}/{total}] Descargando datos de clima para: {empresa}...")
            df_empresa = self.obtener_datos_empresa(empresa, inicio, fin)
            lista_empresas.append(df_empresa)
            print(f"[{i}/{total}] {empresa} completado.")

        return pd.concat(lista_empresas, ignore_index=True)

    def generar_csv_desde_centro(self, inicio: str, fin: str, ruta_salida: str) -> pd.DataFrame:
        empresas = self.obtener_empresas_disponibles()
        df = self.obtener_todas_empresas(empresas, inicio, fin)
        self.guardar_csv(df, ruta_salida)
        return df

    def guardar_csv(self, df: pd.DataFrame, ruta: str):
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        df.to_csv(ruta, index=False, encoding="utf-8")