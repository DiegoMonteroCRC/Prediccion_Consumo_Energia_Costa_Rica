"""Orquesta la unificacion historica entre ARESEP procesado y clima NASA."""

from __future__ import annotations

import glob
from pathlib import Path

import pandas as pd

try:
    from src.api.cliente_api_clima import ClienteAPI
except ModuleNotFoundError:
    from api.cliente_api_clima import ClienteAPI


class GestorDatos:
    """Une fuentes mensuales y genera los CSV derivados usados por el proyecto."""

    COLUMNAS_ARESEP = [
        "Mes",
        "Año",
        "Empresa",
        "Tarifa",
        "Abonados",
        "Ventas",
        "Ingreso sin CVG",
        "Ingreso con CVG",
        "Precio Medio sin CVG",
        "Precio Medio con CVG",
        "Trimestre",
        "Sistema",
        "Trimestral",
    ]

    def __init__(self):
        self.base_dir = Path(__file__).resolve().parents[2]
        self.ruta_aresep = self.base_dir / "data" / "raw" / "aresep" / "*.csv"
        self.ruta_clima_staging = (
            self.base_dir
            / "data"
            / "raw"
            / "api"
            / "clima_NASA_unificado_centrales_electricas_2020-2025.csv"
        )
        self.ruta_clima_legacy = (
            self.base_dir / "data" / "raw" / "api" / "clima_nasa_2020_2025.csv"
        )
        self.ruta_centro = self._resolver_ruta_entrada(
            "Centro.csv",
            subcarpeta="aresep_apis",
        )
        self.ruta_procesados = self.base_dir / "data" / "processed"
        self.ruta_raw = self.base_dir / "data" / "raw"
        self.inicio_clima = "2020"
        self.fin_clima = "2025"
        self.cliente_clima = ClienteAPI(ruta_centro=self.ruta_centro)

    def _resolver_ruta_entrada(self, nombre_archivo: str, subcarpeta: str | None = None) -> Path:
        base_raw = self.base_dir / "data" / "raw"
        base_processed = self.base_dir / "data" / "processed"

        if subcarpeta is None:
            candidatas = [
                base_raw / nombre_archivo,
                base_processed / nombre_archivo,
            ]
        else:
            candidatas = [
                base_raw / subcarpeta / nombre_archivo,
                base_processed / subcarpeta / nombre_archivo,
            ]

        for ruta in candidatas:
            if ruta.exists():
                return ruta

        return candidatas[0]

    @staticmethod
    def _normalizar_columna_ano(df: pd.DataFrame) -> pd.DataFrame:
        if "AÃ±o" in df.columns and "Año" not in df.columns:
            df = df.rename(columns={"AÃ±o": "Año"})
        return df

    def cargar_aresep(self):
        """Lee todos los CSV crudos de ARESEP y los concatena en un solo DataFrame."""
        archivos = glob.glob(str(self.ruta_aresep))
        if not archivos:
            raise FileNotFoundError(
                f"No se encontraron archivos CSV en {self.ruta_aresep.parent}"
            )

        lista_df = []
        for archivo in archivos:
            lista_df.append(pd.read_csv(archivo, encoding="utf-8"))

        return pd.concat(lista_df, ignore_index=True)

    def limpiar_aresep(self, df):
        """Normaliza columnas clave para que ARESEP pueda unirse con clima."""
        df = self._normalizar_columna_ano(df.copy())
        df.columns = df.columns.str.strip()

        if "Empresa" in df.columns:
            df["Empresa"] = df["Empresa"].astype(str).str.strip().str.upper()

        if "Tarifa" in df.columns:
            df["Tarifa"] = df["Tarifa"].astype(str).str.strip().str.upper()

        meses = {
            "ENERO": 1,
            "FEBRERO": 2,
            "MARZO": 3,
            "ABRIL": 4,
            "MAYO": 5,
            "JUNIO": 6,
            "JULIO": 7,
            "AGOSTO": 8,
            "SETIEMBRE": 9,
            "SEPTIEMBRE": 9,
            "OCTUBRE": 10,
            "NOVIEMBRE": 11,
            "DICIEMBRE": 12,
        }

        if "Mes" in df.columns:
            df["Mes"] = df["Mes"].astype(str).str.strip().str.upper().map(meses)

        if "Año" in df.columns:
            df["Año"] = pd.to_numeric(df["Año"], errors="coerce")

        for columna in ["Ventas", "Ingreso con CVG", "Precio Medio con CVG"]:
            if columna in df.columns:
                df[columna] = pd.to_numeric(df[columna], errors="coerce")

        if "Empresa" in df.columns:
            df = df[df["Empresa"] != "TOTAL NACIONAL"]

        df = df.dropna(subset=["Año", "Mes", "Empresa"])
        df["Año"] = df["Año"].astype(int)
        df["Mes"] = df["Mes"].astype(int)

        columnas_existentes = [
            columna for columna in self.COLUMNAS_ARESEP if columna in df.columns
        ]
        return df[columnas_existentes].copy()

    def _csv_clima_granular_valido(self) -> bool:
        ruta_existente = self._resolver_ruta_csv_clima()
        if ruta_existente is None:
            return False

        encabezado = pd.read_csv(ruta_existente, nrows=0, encoding="utf-8")
        encabezado = self._normalizar_columna_ano(encabezado)
        columnas_requeridas = set(ClienteAPI.COLUMNAS_SALIDA_UBICACION)
        return columnas_requeridas.issubset(encabezado.columns)

    def _resolver_ruta_csv_clima(self) -> Path | None:
        for ruta in [self.ruta_clima_staging, self.ruta_clima_legacy]:
            if ruta.exists():
                return ruta
        return None

    def _ordenar_clima_staging(self, df: pd.DataFrame) -> pd.DataFrame:
        columnas_ordenadas = [
            columna
            for columna in ClienteAPI.COLUMNAS_SALIDA_UBICACION
            if columna in df.columns
        ]
        return df[columnas_ordenadas].copy()

    def _guardar_salidas_clima_staging(self, df_clima_staging: pd.DataFrame):
        df_ordenado = self._ordenar_clima_staging(df_clima_staging)
        self.ruta_clima_staging.parent.mkdir(parents=True, exist_ok=True)
        df_ordenado.to_csv(self.ruta_clima_staging, index=False, encoding="utf-8")
        df_ordenado.to_csv(self.ruta_clima_legacy, index=False, encoding="utf-8")

    def generar_clima_si_no_existe(self):
        if self._csv_clima_granular_valido():
            print("Clima: archivo granular existente")
            return

        print("Clima: generando archivo granular desde Centro.csv...")
        df_clima_staging = self.cliente_clima.generar_csv_desde_centro(
            inicio=self.inicio_clima,
            fin=self.fin_clima,
            ruta_salida=self.ruta_clima_staging,
        )
        self._guardar_salidas_clima_staging(df_clima_staging)
        print("Clima: archivo granular generado")

    def cargar_clima_por_ubicacion(self):
        """Carga el CSV granular y valida su contrato minimo."""
        self.generar_clima_si_no_existe()

        ruta_clima = self._resolver_ruta_csv_clima()
        if ruta_clima is None:
            raise FileNotFoundError(
                "No se encontro ningun archivo de clima granular en las rutas esperadas."
            )

        df_clima = pd.read_csv(ruta_clima, encoding="utf-8")
        df_clima = self._normalizar_columna_ano(df_clima)
        df_clima.columns = df_clima.columns.str.strip()

        faltantes = [
            columna
            for columna in ClienteAPI.COLUMNAS_SALIDA_UBICACION
            if columna not in df_clima.columns
        ]
        if faltantes:
            raise KeyError(
                "El CSV granular de clima no contiene las columnas requeridas: "
                f"{faltantes}"
            )

        df_clima["Empresa"] = df_clima["Empresa"].astype(str).str.strip().str.upper()
        df_clima["id_Objecto"] = pd.to_numeric(df_clima["id_Objecto"], errors="coerce")
        df_clima["coordenadaX"] = pd.to_numeric(df_clima["coordenadaX"], errors="coerce")
        df_clima["coordenadaY"] = pd.to_numeric(df_clima["coordenadaY"], errors="coerce")
        df_clima["Año"] = pd.to_numeric(df_clima["Año"], errors="coerce")
        df_clima["Mes"] = pd.to_numeric(df_clima["Mes"], errors="coerce")
        df_clima = df_clima.dropna(
            subset=["id_Objecto", "Empresa", "coordenadaX", "coordenadaY", "Año", "Mes"]
        )
        df_clima["id_Objecto"] = df_clima["id_Objecto"].astype(int)
        df_clima["Año"] = df_clima["Año"].astype(int)
        df_clima["Mes"] = df_clima["Mes"].astype(int)
        df_clima = self._ordenar_clima_staging(df_clima)
        self._guardar_salidas_clima_staging(df_clima)
        return df_clima

    def cargar_clima_staging(self):
        """Entrega el DF oficial de clima que alimenta CSV y staging."""
        return self.cargar_clima_por_ubicacion()

    def cargar_clima_analitico(self):
        """Construye el agregado legacy empresa-mes solo para verificacion analitica."""
        df_clima_ubicacion = self.cargar_clima_staging()
        return self.cliente_clima.agregar_por_empresa_mes(df_clima_ubicacion)

    def unir_datos(self, df_aresep, df_clima):
        """Construye el dataset final usando empresa, anio y mes como llave comun."""
        return pd.merge(df_aresep, df_clima, on=["Empresa", "Año", "Mes"], how="left")

    def guardar_csv(self, df, nombre_archivo):
        destino_por_archivo = {
            "aresep_unificado_2020_2025.csv": self.ruta_raw,
            "dataset_final_2020_2025.csv": self.ruta_procesados,
        }
        carpeta_destino = destino_por_archivo.get(nombre_archivo, self.ruta_procesados)
        carpeta_destino.mkdir(parents=True, exist_ok=True)
        ruta_salida = carpeta_destino / nombre_archivo
        df.to_csv(ruta_salida, index=False, encoding="utf-8")

    def validar_total_nacional(self, df):
        """Compara TOTAL NACIONAL contra la suma de empresas antes de limpiar."""
        df = self._normalizar_columna_ano(df.copy())
        df.columns = df.columns.str.strip()
        df["Empresa"] = df["Empresa"].astype(str).str.strip().str.upper()

        if "Año" in df.columns:
            df["Año"] = pd.to_numeric(df["Año"], errors="coerce")

        if "Ingreso con CVG" in df.columns:
            df["Ingreso con CVG"] = pd.to_numeric(df["Ingreso con CVG"], errors="coerce")

        df = df.dropna(subset=["Año", "Empresa", "Ingreso con CVG"])
        df_total = df[df["Empresa"] == "TOTAL NACIONAL"].copy()
        df_empresas = df[df["Empresa"] != "TOTAL NACIONAL"].copy()

        if df_total.empty:
            print("\nNo existe TOTAL NACIONAL en los archivos.")
            return

        suma_empresas = (
            df_empresas.groupby(["Año", "Mes"], as_index=False)["Ingreso con CVG"]
            .sum()
            .rename(columns={"Ingreso con CVG": "Ingreso_empresas"})
        )

        total_nacional = (
            df_total.groupby(["Año", "Mes"], as_index=False)["Ingreso con CVG"]
            .sum()
            .rename(columns={"Ingreso con CVG": "Ingreso_total"})
        )

        comparacion = pd.merge(
            suma_empresas,
            total_nacional,
            on=["Año", "Mes"],
            how="inner",
        )
        comparacion["Diferencia"] = comparacion["Ingreso_total"] - comparacion["Ingreso_empresas"]
        comparacion["Diferencia_Millones"] = (comparacion["Diferencia"] / 1_000_000).round(2)
        comparacion["Diferencia_Porcentual%"] = (
            (comparacion["Diferencia"] / comparacion["Ingreso_total"]) * 100
        ).round(2)

        comparacion_mostrar = comparacion.copy()
        comparacion_mostrar["Ingreso_empresas"] = comparacion_mostrar["Ingreso_empresas"].apply(
            lambda valor: f"{valor:,.0f}"
        )
        comparacion_mostrar["Ingreso_total"] = comparacion_mostrar["Ingreso_total"].apply(
            lambda valor: f"{valor:,.0f}"
        )
        comparacion_mostrar["Diferencia"] = comparacion_mostrar["Diferencia"].apply(
            lambda valor: f"{valor:,.0f}"
        )
        comparacion_mostrar["Diferencia_Millones"] = comparacion_mostrar[
            "Diferencia_Millones"
        ].apply(lambda valor: f"{valor:,.2f}")
        comparacion_mostrar["Diferencia_Porcentual%"] = comparacion_mostrar[
            "Diferencia_Porcentual%"
        ].apply(lambda valor: f"{valor:.2f}%")

        print("\nValidacion de TOTAL NACIONAL:")
        print(
            comparacion_mostrar[
                [
                    "Año",
                    "Mes",
                    "Ingreso_empresas",
                    "Ingreso_total",
                    "Diferencia",
                    "Diferencia_Millones",
                    "Diferencia_Porcentual%",
                ]
            ].head(12)
        )

    def procesar_todo(self):
        """Ejecuta la secuencia completa: validar, limpiar, unir y exportar."""
        df_aresep_original = self.cargar_aresep()
        self.validar_total_nacional(df_aresep_original)

        df_aresep = self.limpiar_aresep(df_aresep_original)
        df_clima_staging = self.cargar_clima_staging()
        df_clima_analitico = self.cliente_clima.agregar_por_empresa_mes(df_clima_staging)
        df_final = self.unir_datos(df_aresep, df_clima_analitico)

        self.guardar_csv(df_aresep, "aresep_unificado_2020_2025.csv")
        return df_aresep, df_clima_staging, df_final
