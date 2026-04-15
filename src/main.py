import os
from src.api.cliente_api import ClienteAPI
from datos.gestor_datos import GestorDatos

def main():
    ruta_clima = "../data/raw/api/clima_nasa_2020_2025.csv"

    empresas = [
        "CNFL",
        "COOPESANTOS",
        "COOPELESCA",
        "COOPEGUANACASTE",
        "COOPEALFARORUIZ",
        "ESPH",
        "JASEC",
        "ICE"
    ]

    # ========================
    # API: solo si no existe el archivo
    # ========================
    if not os.path.exists(ruta_clima):
        print("No existe el archivo de clima. Descargando desde la API")

        cliente = ClienteAPI()
        df_clima = cliente.obtener_todas_empresas(empresas, "2020", "2025")

        print("\nDatos de clima descargados:")
        print(df_clima.head())
        print(df_clima.shape)

        cliente.guardar_csv(df_clima, ruta_clima)
        print(f"\nArchivo guardado en: {ruta_clima}")

    else:
        print(f"El archivo de clima ya existe: {ruta_clima}")
        print("No se volverá a generar archivo desde la API.")
    # ========================
    #Limpieza y unificación de archivos csv de clima y aresep
    # ========================
    print("\nProcesando datos...")

    gestor = GestorDatos()
    df_aresep, df_clima, df_final = gestor.procesar_todo()

    print("\nARESEP unificado:")
    print(df_aresep.head())
    print(df_aresep.shape)

    print("\nDataset final:")
    print(df_final.head())
    print(df_final.shape)

if __name__ == "__main__":
    main()