import os

from visualizacion.cargador_mapa import CargadorMapa
from visualizacion.mapa_concesiones import MapaConcesiones

def ejecutar_visualizacion():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    ruta_csv = os.path.join(BASE_DIR, "data", "raw", "aresep", "zonas_concesion_operador.csv")
    ruta_salida = os.path.join(BASE_DIR, "data", "processed", "mapas", "mapa_zonas_concesion.html")

    cargador = CargadorMapa(ruta_csv)
    gdf = cargador.obtener_geodatos()

    print("GeoDataFrame cargado correctamente")
    print(gdf[["operador", "descripcion", "area"]].head())

    mapa = MapaConcesiones(gdf)
    mapa.crear_mapa_base()
    mapa.agregar_poligonos()
    mapa.agregar_leyenda()
    mapa.guardar_mapa(ruta_salida)

    print(f"Mapa generado en: {ruta_salida}")


if __name__ == "__main__":
    ejecutar_visualizacion()