import requests
import pandas as pd


class ClienteAPI:
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
        empresa = empresa.strip().upper()
        puntos = self.coordenadas[empresa]
        lista_df = []

        for punto in puntos:
            params = {
                "parameters": "T2M,WS10M,CLOUD_AMT",
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
                "CLOUD_AMT": list(parametros["CLOUD_AMT"].values())
            })

            df["fecha"] = df["fecha"].astype(str)

            # quitar el mes 13 que representa el acumulado/anual
            df = df[df["fecha"].str[-2:] != "13"]

            df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m")
            df["Año"] = df["fecha"].dt.year
            df["Mes"] = df["fecha"].dt.month
            df["Empresa"] = empresa

            lista_df.append(df)

        df_final = pd.concat(lista_df, ignore_index=True)

        df_final = df_final.groupby(["Empresa", "Año", "Mes"], as_index=False)[
            ["T2M", "WS10M", "CLOUD_AMT"]
        ].mean()

        return df_final

    def obtener_todas_empresas(self, empresas, inicio, fin):
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