"""Cliente NASA POWER para construir el historico mensual de clima por empresa."""

import requests
import pandas as pd


class ClienteAPI:
    """Consulta varias coordenadas por empresa y promedia su clima mensual."""

    def __init__(self):
        self.url = "https://power.larc.nasa.gov/api/temporal/monthly/point"

        self.coordenadas = {
            "CNFL": [
                {"lat": 9.9281, "lon": -84.0907},
                {"lat": 9.9980, "lon": -84.1165},
                {"lat": 10.0163, "lon": -84.2116}
            ],
            "COOPESANTOS": [
                {"lat": 9.6600, "lon": -84.0210},
                {"lat": 9.6547, "lon": -83.9703}
            ],
            "COOPELESCA": [
                {"lat": 10.3238, "lon": -84.4271},
                {"lat": 11.0322, "lon": -84.7060},
                {"lat": 10.4317, "lon": -84.0069}
            ],
            "COOPEGUANACASTE": [
                {"lat": 10.1483, "lon": -85.4520},
                {"lat": 10.2600, "lon": -85.5850}
            ],
            "COOPEALFARORUIZ": [
                {"lat": 10.1846, "lon": -84.3916}
            ],
            "ESPH": [
                {"lat": 9.9980, "lon": -84.1165},
                {"lat": 10.0076, "lon": -84.0437}
            ],
            "JASEC": [
                {"lat": 9.8644, "lon": -83.9194}
            ],
            "ICE": [
                {"lat": 10.6350, "lon": -85.4377},
                {"lat": 9.3724, "lon": -83.7037},
                {"lat": 9.9907, "lon": -83.0359}
            ]
        }

    def obtener_datos_empresa(self, empresa, inicio, fin):
        """Descarga puntos de una empresa y consolida un promedio mensual comun."""
        # Varias coordenadas representan una misma empresa para evitar sesgo por un solo punto.
        empresa = empresa.strip().upper()
        puntos = self.coordenadas[empresa]
        lista_df = []

        for punto in puntos:
            params = {
                "parameters": "T2M,WS10M,CLOUD_AMT,RH2M,T2M_MAX,T2M_MIN,CLOUD_OD,GWETROOT,TS,PRECTOTCORR,ALLSKY_SFC_SW_DWN,PS,T2MWET,ALLSKY_SFC_SW_DIFF,ALLSKY_SFC_LW_DWN",
                "community": "RE",
                "longitude": punto["lon"],
                "latitude": punto["lat"],
                "start": inicio,
                "end": fin,
                "format": "json"
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
                "ALLSKY_SFC_LW_DWN": list(parametros["ALLSKY_SFC_LW_DWN"].values())
            })

            df["fecha"] = df["fecha"].astype(str)

            # El mes 13 representa el acumulado anual y se excluye del historico mensual.
            df = df[df["fecha"].str[-2:] != "13"]

            df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m")
            df["Año"] = df["fecha"].dt.year
            df["Mes"] = df["fecha"].dt.month
            df["Empresa"] = empresa

            lista_df.append(df)

        df_final = pd.concat(lista_df, ignore_index=True)
        # El promedio final resume todos los puntos en una sola observacion empresa-mes.

        df_final = df_final.groupby(["Empresa", "Año", "Mes"], as_index=False)[
            [
                "T2M",
                "WS10M",
                "CLOUD_AMT",
                "RH2M",
                "T2M_MAX",
                "T2M_MIN",
                "CLOUD_OD",
                "GWETROOT",
                "TS",
                "PRECTOTCORR",
                "ALLSKY_SFC_SW_DWN",
                "PS",
                "T2MWET",
                "ALLSKY_SFC_SW_DIFF",
                "ALLSKY_SFC_LW_DWN"
            ]
        ].mean()

        return df_final

    def obtener_todas_empresas(self, empresas, inicio, fin):
        """Itera empresa por empresa para dejar trazabilidad de la descarga."""
        lista_empresas = []

        total = len(empresas)

        for i, empresa in enumerate(empresas, start=1):
            print(f"[{i}/{total}] Descargando datos de clima para: {empresa}...")

            df_empresa = self.obtener_datos_empresa(empresa, inicio, fin)
            lista_empresas.append(df_empresa)

            print(f"[{i}/{total}] {empresa} completado.")

        return pd.concat(lista_empresas, ignore_index=True)

    def guardar_csv(self, df, ruta):
        df.to_csv(ruta, index=False, encoding="utf-8")
