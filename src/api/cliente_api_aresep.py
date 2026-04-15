import pandas as pd
import requests
from src.datos.CargadorDatos import CargadorDatos as cd


class _ClienteAPIAresepBase(cd):
    def __init__(self, url):
        super().__init__()
        self.url = url

    def _solicitar_json(self):
        respuesta = requests.get(self.url, timeout=60)
        respuesta.raise_for_status()

        datos = respuesta.json()

        if not isinstance(datos, dict):
            raise ValueError("La respuesta de ARESEP no tiene la estructura esperada.")

        metadata = datos.get("metadata", {})
        if metadata.get("success") is False:
            mensaje = metadata.get("message", "La API de ARESEP devolvio un error.")
            raise ValueError(mensaje)

        registros = datos.get("value", [])
        if not isinstance(registros, list):
            raise ValueError("La respuesta de ARESEP no contiene una lista de registros en 'value'.")

        return registros

    def _acomodar_dataframe(self, columnas_ordenadas=None, columnas_fecha=None, columnas_texto=None):
        registros = self._solicitar_json()
        df = pd.DataFrame(registros)

        if df.empty:
            if columnas_ordenadas:
                return pd.DataFrame(columns=columnas_ordenadas)
            return df

        if columnas_texto:
            for columna in columnas_texto:
                if columna in df.columns:
                    df[columna] = df[columna].apply(
                        lambda valor: valor.strip() if isinstance(valor, str) else valor
                    )

        if columnas_fecha:
            for columna in columnas_fecha:
                if columna in df.columns:
                    df[columna] = pd.to_datetime(df[columna], errors="coerce")

        if columnas_ordenadas:
            columnas_existentes = [columna for columna in columnas_ordenadas if columna in df.columns]
            columnas_restantes = [columna for columna in df.columns if columna not in columnas_existentes]
            df = df[columnas_existentes + columnas_restantes]

        return df


class ClienteAPITarifasElectricidadDistribucion(_ClienteAPIAresepBase):
    def __init__(self):
        super().__init__(
            "https://datos.aresep.go.cr/ws.datosabiertos/Services/IE/TarifasElectricidad.svc/ObtenerTarifasElectricidadDistribucion/0"
        )

    def obtener_datos(self):
        columnas = [
            "id_Mes",
            "mes",
            "anho",
            "fecha",
            "empresa",
            "tipoTarifa",
            "descripcionTarifa",
            "bloque",
            "tarifaPromedio",
            "tarifa",
            "pliego",
            "estructuraCostos",
            "numeroExpediente",
            "numeroResolucion",
            "fechaPublicacion"
        ]

        return self._acomodar_dataframe(
            columnas_ordenadas=columnas,
            columnas_fecha=["fecha", "fechaPublicacion"],
            columnas_texto=[
                "mes",
                "empresa",
                "tipoTarifa",
                "descripcionTarifa",
                "bloque",
                "pliego",
                "estructuraCostos",
                "numeroExpediente",
                "numeroResolucion"
            ]
        )


class ClienteAPITarifasPreciosMedios(_ClienteAPIAresepBase):
    def __init__(self):
        super().__init__(
            "https://datos.aresep.go.cr/ws.datosabiertos/Services/IE/TarifasElectricidad.svc/ObtenerTarifasPreciosMedios/0"
        )

    def obtener_datos(self):
        columnas = [
            "id_Mes",
            "mes",
            "anho",
            "empresa",
            "tipoTarifa",
            "abonados",
            "ventas",
            "ingresoSinCVG",
            "ingresoConCVG",
            "precioMedioSinCVG",
            "precioMedioConCVG",
            "trimestre",
            "sistema",
            "trimestral"
        ]

        return self._acomodar_dataframe(
            columnas_ordenadas=columnas,
            columnas_texto=[
                "mes",
                "empresa",
                "tipoTarifa",
                "trimestre",
                "sistema",
                "trimestral"
            ]
        )


class ClienteAPIInformacionCentralesElectricas(_ClienteAPIAresepBase):
    def __init__(self):
        super().__init__(
            "https://datos.aresep.go.cr/ws.datosabiertos/Services/IE/Electricidad.svc/ObtenerInformacionCentralesElectricasPorDistritoMapa"
        )

    def obtener_datos(self):
        columnas = [
            "id_Objecto",
            "operador",
            "centralElectrica",
            "fuente",
            "provincia",
            "canton",
            "distrito",
            "codigoDTA",
            "coordenadaX",
            "coordenadaY"
        ]

        df = self._acomodar_dataframe(
            columnas_ordenadas=columnas,
            columnas_texto=[
                "operador",
                "centralElectrica",
                "fuente",
                "provincia",
                "canton",
                "distrito",
                "codigoDTA",
                "coordenadaX",
                "coordenadaY"
            ]
        )

        for columna in ["coordenadaX", "coordenadaY"]:
            if columna in df.columns:
                df[columna] = pd.to_numeric(df[columna], errors="coerce")

        return df
