"""Entrada principal del ETL operacional: limpia, carga staging y puebla el DW."""

from src.datos.CargadorDatos import CargadorDatos
from src.datos.ETLs import ETLs
from src.api import cliente_api_aresep
from src.datos.gestor_datos_aresep_clima import GestorDatos


import os
from time import perf_counter

os.environ["PGDATABASE"] = "DW_Energia_ML"
os.environ["PGUSER"] = "sa"
os.environ["PGPASSWORD"] = "progra"
os.environ["PGHOST"] = "34.136.178.175"
os.environ["PGPORT"] = "5432"


CENTRO_COLUMNS = [
    "id_Objecto",
    "operador",
    "centralElectrica",
    "fuente",
    "provincia",
    "canton",
    "distrito",
    "codigoDTA",
    "coordenadaX",
    "coordenadaY",
]

ZONAS_COLUMNS = [
    "id_Objecto",
    "operador",
    "descripcion",
    "area",
    "coordenadas",
    "tipo_geometria",
]

DISTRIBUCION_COLUMNS = [
    "id_Mes",
    "mes",
    "anho",
    "empresa",
    "tipoTarifa",
    "descripcionTarifa",
    "bloque",
    "tarifaPromedio",
]

HIDROCARBUROS_COLUMNS = [
    "numeroExpediente",
    "numeroResolucion",
    "fechaPublicacion",
    "alcanceGaceta",
    "numeroGaceta",
    "producto",
    "tipoCambio",
    "precioReferenciaInternacional",
    "precioColonizado",
    "otrosIngresosProrrateados",
    "asigCruzPesc",
    "asigCruzMinae",
    "subCruzPesc",
    "subCruzMinae",
    "diferencialPrecios",
    "impuestoUnico",
    "canon",
    "margenOperacion",
    "rendTarif",
    "precioPlantelSinImpuesto",
    "precioConImpuesto",
    "margenEstacionesTerrestres",
    "margenEstacionesAereas",
    "fleteEstaciones",
    "margenEnvasador",
    "margenDistribuidos",
    "margenDetallista",
    "rige",
    "precioConsumidorFinalGas",
    "precioFinal",
    "precioFinalSinPuntoFijo",
]

ARESEP_MEDIOS_COLUMNS = [
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

CLIMA_COLUMNS = [
    "Empresa",
    "Año",
    "Mes",
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


def checkpoint(inicio_total, inicio_etapa, mensaje):
    """Mide el avance por etapa para ubicar cuellos de botella del pipeline."""
    ahora = perf_counter()
    print(
        f"[CHECKPOINT] {mensaje} | etapa: {ahora - inicio_etapa:.2f}s | total: {ahora - inicio_total:.2f}s"
    )
    return ahora


def _ordenar_columnas_existentes(df, columnas):
    columnas_existentes = [col for col in columnas if col in df.columns]
    return df[columnas_existentes].copy()


def preparar_centro(etl):
    """Aplica la limpieza minima que replica el notebook de centrales."""
    etl.convert_lon_lat(col_lon="coordenadaX", col_lat="coordenadaY")
    etl.df = _ordenar_columnas_existentes(etl.df, CENTRO_COLUMNS)
    return etl


def preparar_zonas(etl):
    """Replica el notebook de zonas agregando el tipo de geometria sin alterar la WKT."""
    etl.split_col(columna="coordenadas", extract_index=0, new_col="tipo_geometria", rm=False)
    etl.df = _ordenar_columnas_existentes(etl.df, ZONAS_COLUMNS)
    return etl


def preparar_distribucion(etl):
    """Conserva las columnas utiles del flujo de distribucion antes del staging."""
    columnas_sin_nulos = list(etl.col_nulls(reverse=True, chain=False).keys())
    columnas_filtradas = [col for col in DISTRIBUCION_COLUMNS if col in columnas_sin_nulos]
    etl.df = _ordenar_columnas_existentes(etl.df, columnas_filtradas)
    return etl


def preparar_hidrocarburos(etl):
    """Replica la limpieza principal del notebook de hidrocarburos."""
    etl.rm_col(["observaciones"], axis=1)
    etl.rm_col(["idSIET"], axis=1)
    etl.df = _ordenar_columnas_existentes(etl.df, HIDROCARBUROS_COLUMNS)
    num_col = list(etl.numeric_col(chain=False).columns.values)
    etl.ceros_nan(to_cero=True, columnas=num_col)
    return etl


def preparar_aresep_medios(etl):
    """Alinea el DataFrame de medios con el layout esperado por el staging."""
    etl.df = _ordenar_columnas_existentes(etl.df, ARESEP_MEDIOS_COLUMNS)
    return etl


def preparar_clima(etl):
    """Alinea el clima consolidado con el orden de columnas del modelo staging."""
    etl.df = _ordenar_columnas_existentes(etl.df, CLIMA_COLUMNS)
    return etl


def main():
    inicio_total = perf_counter()
    inicio_etapa = inicio_total

    # La corrida inicia con staging vacio para que los hechos se recarguen sin residuos previos.
    print("[INICIO] Limpiando staging...")
    cl_stg = ETLs()
    cl_stg.clear_staging()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Staging limpiado")

    # Los catalogos se pueblan primero porque no dependen de DataFrames operativos.
    print("[INICIO] Poblando catalogos...")
    ETLs().etl_catalogos()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Catalogos poblados")

    # Estas APIs generan las fuentes crudas que luego se alinean con los notebooks de limpieza.
    print("[INICIO] Extrayendo datos ARESEP...")
    centrales = cliente_api_aresep.ClienteAPIInformacionCentralesElectricas()
    zonas = cliente_api_aresep.ClienteAPIZonasConcesionPorOperador()
    distribucion = cliente_api_aresep.ClienteAPITarifasElectricidadDistribucion()
    hidrocarburos = cliente_api_aresep.ClienteAPIHistoricoTarifasHidrocarburos()

    cent = centrales.obtener_datos(chain=False)
    zon = zonas.obtener_datos(chain=False)
    dist = distribucion.obtener_datos(chain=False)
    hc = hidrocarburos.obtener_datos(chain=False)
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Extraccion ARESEP completada")

    print(
        f"[DATA] Centro: {cent.shape} | Zonas: {zon.shape} | Distribucion: {dist.shape} | Hidrocarburos: {hc.shape}"
    )

    # GestorDatos produce los datasets consolidados que alimentan clima y medios.
    print("[INICIO] Construyendo datasets unificados de clima y medios...")
    etl_clima = ETLs()
    etl_hidrocarburos = ETLs()
    etl_medios = ETLs()
    etl_distribucion = ETLs()
    etl_centrales = ETLs()
    etl_zonas = ETLs()

    gestor = GestorDatos()
    df_aresep, df_clima, _ = gestor.procesar_todo()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Datasets derivados construidos")

    print(
        f"[DATA] Medios: {df_aresep.shape} | Clima: {df_clima.shape}"
    )

    etl_clima.df = df_clima
    etl_hidrocarburos.df = hc
    etl_medios.df = df_aresep
    etl_distribucion.df = dist
    etl_centrales.df = cent
    etl_zonas.df = zon

    # Cada dominio se prepara por separado para respetar su grano antes del insert.
    print("[INICIO] Preparando y cargando hidrocarburos a staging...")
    preparar_hidrocarburos(etl_hidrocarburos).etl_stg_hidrocarburos()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Hidrocarburos cargado a staging")

    print("[INICIO] Preparando y cargando centrales a staging...")
    preparar_centro(etl_centrales).etl_stg_centro()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Centro cargado a staging")

    print("[INICIO] Preparando y cargando zonas a staging...")
    preparar_zonas(etl_zonas).etl_stg_zonas()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Zonas cargadas a staging")

    print("[INICIO] Preparando y cargando distribucion a staging...")
    preparar_distribucion(etl_distribucion).etl_stg_distribucion()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Distribucion cargada a staging")

    print("[INICIO] Preparando y cargando medios a staging...")
    preparar_aresep_medios(etl_medios).etl_stg_aresep_medios()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Medios cargado a staging")

    print("[INICIO] Preparando y cargando clima a staging...")
    preparar_clima(etl_clima).etl_stg_clima_nasa()
    inicio_etapa = checkpoint(inicio_total, inicio_etapa, "Clima cargado a staging")

    # La ultima etapa SQL toma staging como fuente para poblar dimensiones y hechos.
    print("[INICIO] Poblando dimensiones y hechos...")
    CargadorDatos().cargar_a_fact_dim()
    checkpoint(inicio_total, inicio_etapa, "Fact_Dim poblado")

    print("#" * 10, f"\nListo en {perf_counter() - inicio_total:.2f}s\n", "#" * 10)


if __name__ == "__main__":
    main()
