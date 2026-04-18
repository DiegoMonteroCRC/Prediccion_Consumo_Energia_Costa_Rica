"""Orquesta la unificacion historica entre ARESEP procesado y clima NASA."""

import os
import glob
import pandas as pd


class GestorDatos:
    """Une fuentes mensuales y genera los CSV derivados usados por el proyecto."""

    def __init__(self):
        self.ruta_aresep = "../data/raw/aresep/*.csv"
        self.ruta_clima = "../data/raw/api/clima_nasa_2020_2025.csv"
        self.ruta_procesados = "../data/processed/"

    def cargar_aresep(self):
        """Lee todos los CSV crudos de ARESEP y los concatena en un solo DataFrame."""
        # La union temprana simplifica la validacion y la limpieza mensual posterior.
        archivos = glob.glob(self.ruta_aresep)

        if not archivos:
            raise FileNotFoundError("No se encontraron archivos CSV en ../data/raw/aresep/")

        lista_df = []

        for archivo in archivos:
            df = pd.read_csv(archivo, encoding="utf-8")
            lista_df.append(df)

        df_aresep = pd.concat(lista_df, ignore_index=True)
        return df_aresep

    def limpiar_aresep(self, df):
        """Normaliza columnas clave para que ARESEP pueda unirse con clima."""
        # Aqui se homologa texto, meses y tipos antes del merge con NASA POWER.
        df = df.copy()

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
            "DICIEMBRE": 12
        }

        if "Mes" in df.columns:
            df["Mes"] = df["Mes"].astype(str).str.strip().str.upper()
            df["Mes"] = df["Mes"].map(meses)

        if "Año" in df.columns:
            df["Año"] = pd.to_numeric(df["Año"], errors="coerce")

        columnas_numericas = ["Ventas", "Ingreso con CVG", "Precio Medio con CVG"]

        for col in columnas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # eliminar TOTAL NACIONAL para el análisis principal
        if "Empresa" in df.columns:
            df = df[df["Empresa"] != "TOTAL NACIONAL"]

        df = df.dropna(subset=["Año", "Mes", "Empresa"])

        df["Año"] = df["Año"].astype(int)
        df["Mes"] = df["Mes"].astype(int)

        return df

    def cargar_clima(self):
        if not os.path.exists(self.ruta_clima):
            raise FileNotFoundError("No se encontró el archivo de clima en ../data/raw/api/")

        df_clima = pd.read_csv(self.ruta_clima, encoding="utf-8")
        df_clima.columns = df_clima.columns.str.strip()

        df_clima["Empresa"] = df_clima["Empresa"].astype(str).str.strip().str.upper()
        df_clima["Año"] = pd.to_numeric(df_clima["Año"], errors="coerce")
        df_clima["Mes"] = pd.to_numeric(df_clima["Mes"], errors="coerce")

        df_clima = df_clima.dropna(subset=["Empresa", "Año", "Mes"])

        df_clima["Año"] = df_clima["Año"].astype(int)
        df_clima["Mes"] = df_clima["Mes"].astype(int)

        return df_clima

    def unir_datos(self, df_aresep, df_clima):
        """Construye el dataset final usando empresa, anio y mes como llave comun."""
        df_final = pd.merge(
            df_aresep,
            df_clima,
            on=["Empresa", "Año", "Mes"],
            how="left"
        )
        return df_final

    def guardar_csv(self, df, nombre_archivo):
        os.makedirs(self.ruta_procesados, exist_ok=True)
        ruta_salida = os.path.join(self.ruta_procesados, nombre_archivo)
        df.to_csv(ruta_salida, index=False, encoding="utf-8")

    def validar_total_nacional(self, df):
        """Compara TOTAL NACIONAL contra la suma de empresas antes de limpiar."""
        df = df.copy()

        df.columns = df.columns.str.strip()
        df["Empresa"] = df["Empresa"].astype(str).str.strip().str.upper()

        # convertir columnas importantes
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
            how="inner"
        )

        comparacion["Diferencia"] = comparacion["Ingreso_total"] - comparacion["Ingreso_empresas"]

        comparacion["Diferencia_Millones"] = (comparacion["Diferencia"] / 1_000_000).round(2)

        comparacion["Diferencia_Porcentual%"] = (
            (comparacion["Diferencia"] / comparacion["Ingreso_total"]) * 100
        ).round(2)

        # crear columnas solo para mostrar bonito
        comparacion_mostrar = comparacion.copy()
        comparacion_mostrar["Ingreso_empresas"] = comparacion_mostrar["Ingreso_empresas"].apply(
            lambda x: f"{x:,.0f}"
        )
        comparacion_mostrar["Ingreso_total"] = comparacion_mostrar["Ingreso_total"].apply(
            lambda x: f"{x:,.0f}"
        )
        comparacion_mostrar["Diferencia"] = comparacion_mostrar["Diferencia"].apply(
            lambda x: f"{x:,.0f}"
        )
        comparacion_mostrar["Diferencia_Millones"] = comparacion_mostrar["Diferencia_Millones"].apply(
            lambda x: f"{x:,.2f}"
        )
        comparacion_mostrar["Diferencia_Porcentual%"] = comparacion_mostrar["Diferencia_Porcentual%"].apply(
            lambda x: f"{x:.2f}%"
        )

        print("\nValidación de TOTAL NACIONAL:")
        print(
            comparacion_mostrar[
                [
                    "Año",
                    "Mes",
                    "Ingreso_empresas",
                    "Ingreso_total",
                    "Diferencia",
                    "Diferencia_Millones",
                    "Diferencia_Porcentual%"
                ]
            ].head(12)
        )

    def procesar_todo(self):
        """Ejecuta la secuencia completa: validar, limpiar, unir y exportar."""
        # Este metodo es la fuente de los CSV derivados usados por notebooks y ETL_main.
        df_aresep_original = self.cargar_aresep()

        # validar antes de eliminar TOTAL NACIONAL
        self.validar_total_nacional(df_aresep_original)

        df_aresep = self.limpiar_aresep(df_aresep_original)
        df_clima = self.cargar_clima()
        df_final = self.unir_datos(df_aresep, df_clima)

        self.guardar_csv(df_aresep, "aresep_unificado_2020_2025.csv")
        self.guardar_csv(df_final, "dataset_final_2020_2025.csv")

        return df_aresep, df_clima, df_final
