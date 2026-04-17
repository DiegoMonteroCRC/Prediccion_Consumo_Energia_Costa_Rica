"""Clientes HTTP para las APIs de ARESEP usadas en el ETL."""

import pandas as pd
import requests
from src.datos.CargadorDatos import CargadorDatos


def acomodar_dataframe(df:pd.DataFrame, columnas_ordenadas=None, columnas_fecha=None, columnas_texto=None):
    """Limpia orden, fechas y texto para dejar la respuesta lista para pandas/EDA."""
    # Este helper deja todos los endpoints con una estructura comparable antes de guardarlos.
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


class _ClienteAPIAresepBase(CargadorDatos):
    """Base comun para pedir endpoints ARESEP y devolverlos como DataFrame."""

    def __init__(self, url, fecha_inicio, fecha_final):
        super().__init__()
        self.none = None
        self.url = url
        self.url_base = "https://datos.aresep.go.cr/ws.datosabiertos/Services/IE/"
        self.fecha_inicio = fecha_inicio
        self.fecha_final = fecha_final
        self.anios = (self.fecha_final - self.fecha_inicio) if isinstance(self.fecha_inicio, int) and isinstance(self.fecha_final,int) else self.none

    def _solicitar_json(self, anio = None):
        """Resuelve la URL correcta y valida la estructura JSON esperada."""
        # Algunos servicios exponen un historico anual; otros responden todo en una sola llamada.
        match self.anios:
            case None:
                respuesta = requests.get(f"{self.url_base}{self.url}", timeout=60)
            case 0:
                respuesta = requests.get(f"{self.url_base}{self.url}/{anio}", timeout=60)
            case self.anios:
                respuesta = requests.get(f"{self.url_base}{self.url}/{anio}", timeout=60)

        respuesta.raise_for_status()

        datos = respuesta.json()

        if not isinstance(datos, dict):
            raise ValueError("La respuesta de ARESEP no tiene la estructura esperada.")

        # metadata.success permite cortar rapido cuando ARESEP reporta error de negocio.
        metadata = datos.get("metadata", {})
        if metadata.get("success") is False:
            mensaje = metadata.get("message", "La API de ARESEP devolvio un error.")
            raise ValueError(mensaje)

        registros = datos.get("value", [])
        if not isinstance(registros, list):
            raise ValueError("La respuesta de ARESEP no contiene una lista de registros en 'value'.")

        return registros

    def _iter_dates(self, *args, **kwargs):
        """Concatena respuestas por anio cuando el endpoint expone historicos anuales."""
        lista_registros =[]
        match self.anios:
            case None:
                registros = self._solicitar_json(self.anios)
                df = pd.DataFrame(registros)
                return acomodar_dataframe(df, *args, **kwargs)
            case 0:
                registros = self._solicitar_json(self.anios)
                df = pd.DataFrame(registros)
                return acomodar_dataframe(df, *args, **kwargs)
            case self.anios:
                if self.anios >= 0:
                    for anio in range(self.fecha_inicio, self.fecha_final + 1):
                        registros = self._solicitar_json(anio)
                        df_raw = pd.DataFrame(registros)
                        df = acomodar_dataframe(df_raw, *args, **kwargs)
                        lista_registros.append(df)
                    return pd.concat(lista_registros, ignore_index=False)






class ClienteAPITarifasElectricidadDistribucion(_ClienteAPIAresepBase):
    """Descarga el historico de distribucion electrica y lo ordena por columnas clave."""

    def __init__(self, ):
        super().__init__(
            "TarifasElectricidad.svc/ObtenerTarifasElectricidadDistribucion",
            2019,
            2026
        )

    def obtener_datos(self, chain=True):
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

        df = self._iter_dates(
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
        self.df = df
        return self._chain_response(df, chain)


class ClienteAPITarifasPreciosMedios(_ClienteAPIAresepBase):
    """Descarga precios medios electricos y conserva el layout de negocio."""

    def __init__(self):
        super().__init__(
            "TarifasElectricidad.svc/ObtenerTarifasPreciosMedios",
            2019,
            2026
        )

    def obtener_datos(self, chain=True):
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

        df = self._iter_dates(
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
        self.df = df
        return self._chain_response(df, chain)


class ClienteAPIInformacionCentralesElectricas(_ClienteAPIAresepBase):
    """Descarga el catalogo geografico de centrales electricas."""

    def __init__(self):
        super().__init__(
            "Electricidad.svc/ObtenerInformacionCentralesElectricasPorDistritoMapa",
            None,
            None
        )

    def obtener_datos(self, chain=True):
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

        df = self._iter_dates(
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

        self.df = df
        return self._chain_response(df, chain)


class ClienteAPIHistoricoTarifasHidrocarburos(_ClienteAPIAresepBase):
    """Descarga el historico completo de tarifas de hidrocarburos."""

    def __init__(self):
        super().__init__(
            "TarifaCombustible.svc/ObtenerHistoricoTarifasHidrocarburos",
            None,
            None
        )

    def obtener_datos(self, chain=True):
        columnas = [
            "numeroExpediente",
            "numeroResolucion",
            "fechaPublicacion",
            "alcanceGaceta",
            "numeroGaceta",
            "producto",
            "tipoCambio",
            "precioReferenciaInternacional",
            "precioColonizado",
            "costoAdquisicionDolar",
            "costoAdquisicionColones",
            "otrosIngresosProrrateados",
            "asigCruzPesc",
            "asigCruzMinae",
            "asigPolGob",
            "subCruzPesc",
            "subCruzMinae",
            "subPolGob",
            "diferencialPrecios",
            "liquidacionExtraordinaria",
            "impuestoUnico",
            "canon",
            "margenOperacion",
            "rendTarif",
            "liquidacionOrdinaria",
            "precioPlantelSinImpuesto",
            "ley9840",
            "precioConImpuesto",
            "margenComercionalizador",
            "precioFinalSinPuntoFijo",
            "margenEstacionesTerrestres",
            "margenEstacionesAereas",
            "fleteEstaciones",
            "margenEnvasador",
            "precioFinalSinIVA",
            "IVA",
            "precioFinal",
            "margenDistribuidos",
            "margenDetallista",
            "precioConsumidorFinalGas",
            "precioTerminal",
            "rige",
            "limiteInferiorBanda",
            "limiteSuperiorBanda",
            "densidadReferencia",
            "precioImpuestoMasa",
            "precioFinalSinPuntoFijoMasa",
            "margenComercializadorMasa",
            "idSIET",
            "observaciones"
        ]

        df = self._iter_dates(
            columnas_ordenadas=columnas,
            columnas_fecha=["fechaPublicacion"],
            columnas_texto=[
                "numeroExpediente",
                "numeroResolucion",
                "alcanceGaceta",
                "producto",
                "rige",
                "idSIET",
                "observaciones"
            ]
        )
        self.df = df
        return self._chain_response(df, chain)
