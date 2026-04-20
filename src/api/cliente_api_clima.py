"""Cliente NASA POWER para construir clima mensual por ubicacion y por empresa."""

from __future__ import annotations

import unicodedata
from pathlib import Path

import pandas as pd
import requests


class ClienteAPI:
    """Consulta NASA POWER usando las ubicaciones declaradas en Centro.csv."""

    COLUMNAS_CLIMA = [
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
        "ALLSKY_SFC_LW_DWN",
    ]

    COLUMNAS_CENTRO = [
        "id_Objecto",
        "centralElectrica",
        "operador",
        "coordenadaX",
        "coordenadaY",
    ]

    COLUMNAS_SALIDA_UBICACION = [
        "id_Objecto",
        "centralElectrica",
        "operador",
        "Empresa",
        "coordenadaX",
        "coordenadaY",
        "Año",
        "Mes",
        *COLUMNAS_CLIMA,
    ]

    def __init__(self, ruta_centro: str | Path | None = None):
        self.url = "https://power.larc.nasa.gov/api/temporal/monthly/point"
        self.base_dir = Path(__file__).resolve().parents[2]
        self.ruta_centro = Path(ruta_centro) if ruta_centro is not None else None
        self.ruta_clima_granular = (
            self.base_dir
            / "data"
            / "raw"
            / "api"
            / "clima_NASA_unificado_centrales_electricas_2020-2025.csv"
        )
        self.parametros = ",".join(self.COLUMNAS_CLIMA)
        self.mapa_operadores = {
            self._normalizar_texto("COMPAÑIA NACIONAL DE FUERZA Y LUZ S.A."): "CNFL",
            self._normalizar_texto(
                "COOPERATIVA DE ELECTRIFICACION RURAL LOS SANTOS R.L."
            ): "COOPESANTOS",
            self._normalizar_texto(
                "COOPERATIVA DE ELECTRIFICACION RURAL DE SAN CARLOS R.L."
            ): "COOPELESCA",
            self._normalizar_texto(
                "COOPERATIVA DE ELECTRIFICACION RURAL DE GUANACASTE R.L."
            ): "COOPEGUANACASTE",
            self._normalizar_texto(
                "COOPERATIVA DE ELECTRIFICACIÓN RURAL DE ALFARO RUIZ R.L."
            ): "COOPEALFARORUIZ",
            self._normalizar_texto(
                "COOPERATIVA DE ELECTRIFICACION RURAL DE ALFARO RUIZ R.L."
            ): "COOPEALFARORUIZ",
            self._normalizar_texto(
                "EMPRESA DE SERVICIOS PUBLICOS DE HEREDIA S.A."
            ): "ESPH",
            self._normalizar_texto(
                "JUNTA ADMINISTRATIVA DEL SERVICIO ELECTRICO MUNICIPAL DE CARTAGO"
            ): "JASEC",
            self._normalizar_texto("INSTITUTO COSTARRICENSE DE ELECTRICIDAD"): "ICE",
        }

    def _resolver_ruta_centro(self) -> Path:
        if self.ruta_centro is not None:
            return self.ruta_centro

        candidatas = [
            self.base_dir / "data" / "raw" / "aresep_apis" / "Centro.csv",
            self.base_dir / "data" / "processed" / "aresep_apis" / "Centro.csv",
        ]

        for ruta in candidatas:
            if ruta.exists():
                return ruta

        return candidatas[0]

    @staticmethod
    def _normalizar_texto(valor: str) -> str:
        texto = unicodedata.normalize("NFKD", str(valor).strip().upper())
        return "".join(
            caracter for caracter in texto if not unicodedata.combining(caracter)
        )

    @staticmethod
    def _normalizar_ano_columnas(df: pd.DataFrame) -> pd.DataFrame:
        if "AÃ±o" in df.columns and "Año" not in df.columns:
            return df.rename(columns={"AÃ±o": "Año"})
        if "AÃƒÂ±o" in df.columns and "Año" not in df.columns:
            return df.rename(columns={"AÃƒÂ±o": "Año"})
        return df

    def _resolver_empresa(self, operador: str) -> str | None:
        return self.mapa_operadores.get(self._normalizar_texto(operador))

    def cargar_centro_df(self) -> pd.DataFrame:
        """Carga Centro.csv y deriva el codigo de empresa para cada ubicacion."""
        ruta_centro = self._resolver_ruta_centro()

        if not ruta_centro.exists():
            raise FileNotFoundError(
                f"No se encontro Centro.csv en la ruta esperada: {ruta_centro}"
            )

        df = pd.read_csv(ruta_centro, encoding="utf-8")
        df.columns = df.columns.str.strip()

        faltantes = set(self.COLUMNAS_CENTRO).difference(df.columns)
        if faltantes:
            raise KeyError(
                "Centro.csv no contiene las columnas requeridas: "
                f"{sorted(faltantes)}"
            )

        df = df[self.COLUMNAS_CENTRO].copy()
        df["operador"] = df["operador"].astype(str).str.strip()
        df["Empresa"] = df["operador"].apply(self._resolver_empresa)
        df["coordenadaX"] = pd.to_numeric(df["coordenadaX"], errors="coerce")
        df["coordenadaY"] = pd.to_numeric(df["coordenadaY"], errors="coerce")

        df = df.dropna(subset=["Empresa", "coordenadaX", "coordenadaY"])
        df = df.drop_duplicates(subset=["id_Objecto", "coordenadaX", "coordenadaY"])
        return df.reset_index(drop=True)

    def obtener_empresas_disponibles(self) -> list[str]:
        return sorted(self.cargar_centro_df()["Empresa"].unique().tolist())

    def _cargar_clima_granular_cache(self) -> pd.DataFrame | None:
        if not self.ruta_clima_granular.exists():
            return None

        df = pd.read_csv(self.ruta_clima_granular, encoding="utf-8")
        df.columns = df.columns.str.strip()
        df = self._normalizar_ano_columnas(df)

        faltantes = [
            columna
            for columna in self.COLUMNAS_SALIDA_UBICACION
            if columna not in df.columns
        ]
        if faltantes:
            return None

        return df

    def _consultar_punto(
        self,
        latitud: float,
        longitud: float,
        inicio: str,
        fin: str,
    ) -> pd.DataFrame:
        params = {
            "parameters": self.parametros,
            "community": "RE",
            "longitude": longitud,
            "latitude": latitud,
            "start": inicio,
            "end": fin,
            "format": "json",
        }

        respuesta = requests.get(self.url, params=params, timeout=30)
        respuesta.raise_for_status()
        datos = respuesta.json()
        parametros = datos["properties"]["parameter"]
        fechas = list(parametros["T2M"].keys())

        df = pd.DataFrame(
            {
                "fecha": fechas,
                **{
                    columna: list(parametros[columna].values())
                    for columna in self.COLUMNAS_CLIMA
                },
            }
        )

        df["fecha"] = df["fecha"].astype(str)
        df = df[df["fecha"].str[-2:] != "13"].copy()
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m")
        df["Año"] = df["fecha"].dt.year
        df["Mes"] = df["fecha"].dt.month
        return df.drop(columns=["fecha"])

    def obtener_datos_ubicacion(
        self,
        ubicacion: pd.Series | dict,
        inicio: str,
        fin: str,
    ) -> pd.DataFrame:
        """Descarga el historico mensual de una ubicacion de Centro.csv."""
        fila = ubicacion if isinstance(ubicacion, pd.Series) else pd.Series(ubicacion)
        df = self._consultar_punto(
            latitud=float(fila["coordenadaY"]),
            longitud=float(fila["coordenadaX"]),
            inicio=inicio,
            fin=fin,
        )

        df["id_Objecto"] = fila["id_Objecto"]
        df["centralElectrica"] = fila["centralElectrica"]
        df["operador"] = fila["operador"]
        df["Empresa"] = fila["Empresa"]
        df["coordenadaX"] = fila["coordenadaX"]
        df["coordenadaY"] = fila["coordenadaY"]
        return df[self.COLUMNAS_SALIDA_UBICACION]

    def obtener_clima_por_ubicacion(
        self,
        inicio: str,
        fin: str,
        df_centro: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Descarga el historico mensual por cada ubicacion declarada en Centro.csv."""
        centro = self.cargar_centro_df() if df_centro is None else df_centro.copy()
        total = len(centro)
        lista_df = []

        for indice, (_, fila) in enumerate(centro.iterrows(), start=1):
            print(
                "[{}/{}] Descargando clima para {} ({})...".format(
                    indice,
                    total,
                    fila["centralElectrica"],
                    fila["Empresa"],
                )
            )
            lista_df.append(self.obtener_datos_ubicacion(fila, inicio, fin))

        if not lista_df:
            return pd.DataFrame(columns=self.COLUMNAS_SALIDA_UBICACION)

        return pd.concat(lista_df, ignore_index=True)

    def agregar_por_empresa_mes(self, df_clima_ubicacion: pd.DataFrame) -> pd.DataFrame:
        """Reduce el clima por ubicacion al contrato legacy empresa-mes."""
        df = df_clima_ubicacion.copy()
        df.columns = df.columns.str.strip()
        df = self._normalizar_ano_columnas(df)

        columnas_faltantes = [
            columna
            for columna in ["Empresa", "Año", "Mes", *self.COLUMNAS_CLIMA]
            if columna not in df.columns
        ]
        if columnas_faltantes:
            raise KeyError(
                "El DataFrame de clima por ubicacion no contiene las columnas requeridas: "
                f"{columnas_faltantes}"
            )

        df["Empresa"] = df["Empresa"].astype(str).str.strip().str.upper()
        df["Año"] = pd.to_numeric(df["Año"], errors="coerce")
        df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce")
        df = df.dropna(subset=["Empresa", "Año", "Mes"])
        df["Año"] = df["Año"].astype(int)
        df["Mes"] = df["Mes"].astype(int)

        return (
            df.groupby(["Empresa", "Año", "Mes"], as_index=False)[self.COLUMNAS_CLIMA]
            .mean()
            .sort_values(["Empresa", "Año", "Mes"], ignore_index=True)
        )

    def _obtener_clima_ubicacion_empresas(
        self,
        empresas: list[str],
        inicio: str,
        fin: str,
    ) -> pd.DataFrame:
        empresas_normalizadas = [empresa.strip().upper() for empresa in empresas]
        clima_cache = self._cargar_clima_granular_cache()

        if clima_cache is not None:
            clima_ubicacion = clima_cache[
                clima_cache["Empresa"].isin(empresas_normalizadas)
            ].copy()
        else:
            centro = self.cargar_centro_df()
            centro = centro[centro["Empresa"].isin(empresas_normalizadas)].copy()
            clima_ubicacion = self.obtener_clima_por_ubicacion(
                inicio,
                fin,
                df_centro=centro,
            )

        if clima_ubicacion.empty:
            raise KeyError(
                "No hay ubicaciones configuradas para las empresas solicitadas: "
                f"{empresas_normalizadas}"
            )

        return clima_ubicacion.sort_values(
            ["Empresa", "id_Objecto", "Año", "Mes"],
            ignore_index=True,
        )

    def obtener_datos_empresa(
        self,
        empresa: str,
        inicio: str,
        fin: str,
        agregado: bool = False,
    ) -> pd.DataFrame:
        """Devuelve clima por ubicacion o agregado para una empresa."""
        clima_ubicacion = self._obtener_clima_ubicacion_empresas([empresa], inicio, fin)
        if agregado:
            return self.agregar_por_empresa_mes(clima_ubicacion)
        return clima_ubicacion

    def obtener_todas_empresas(
        self,
        empresas: list[str] | None = None,
        inicio: str = "2020",
        fin: str = "2025",
        agregado: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Devuelve clima por ubicacion o agregado para el conjunto de empresas."""
        if empresas is None:
            empresas = kwargs.get("empresas")
        if empresas is None:
            empresas = self.obtener_empresas_disponibles()

        inicio = str(kwargs.get("inicio", inicio))
        fin = str(kwargs.get("fin", fin))
        agregado = kwargs.get("agregado", agregado)
        clima_ubicacion = self._obtener_clima_ubicacion_empresas(
            empresas,
            inicio,
            fin,
        )
        if agregado:
            return self.agregar_por_empresa_mes(clima_ubicacion)
        return clima_ubicacion

    def generar_csv_desde_centro(
        self,
        inicio: str,
        fin: str,
        ruta_salida: str | Path,
    ) -> pd.DataFrame:
        """Genera y guarda el CSV granular de clima por ubicacion."""
        df = self.obtener_clima_por_ubicacion(inicio, fin)
        self.guardar_csv(df, ruta_salida)
        return df

    def guardar_csv(self, df: pd.DataFrame, ruta: str | Path):
        ruta_destino = Path(ruta)
        if not ruta_destino.is_absolute():
            partes = list(ruta_destino.parts)
            while partes and partes[0] == "..":
                partes.pop(0)
            ruta_destino = self.base_dir.joinpath(*partes) if partes else self.base_dir
        ruta_destino.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(ruta_destino, index=False, encoding="utf-8")
