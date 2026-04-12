# Librerías necesarias 
if (!require("httr")) install.packages("httr")
if (!require("jsonlite")) install.packages("jsonlite")
if (!require("dplyr")) install.packages("dplyr")
if (!require("readr")) install.packages("readr")
if (!require("purrr")) install.packages("purrr")

library(httr); library(jsonlite); library(dplyr); library(readr); library(purrr)

# Coordenadas actualizadas de zonas bananeras
zonas_banano <- data.frame(
  Zona = c("Matina","Siquirres","Valle La Estrella","Cariari","La Rita","Roxana",
           "Guapiles","Guacimo","Sixaola","Talamanca","Sarapiqui","Barra del Colorado",
           "Coto","Palmar Sur","Rio Claro","Golfito","Quepos","Cahuita",
           "Uatsi","Turrialba (banano altura)"),
  Latitud = c(10.00,10.09,9.86,10.22,10.22,10.25,
              10.20,10.21,9.50,9.58,
              10.40,10.76,
              8.62,8.96,8.71,8.50,9.45,9.75,9.65,9.90),
  Longitud = c(-83.35,-83.50,-83.00,-83.77,-83.77,-83.72,
               -83.79,-83.67,-82.85,-82.95,
               -83.80,-83.58,
               -82.95,-83.48,-83.06,-83.20,-84.15,-83.20,-82.95,-83.75)
)

# Variables y parámetros
variables <- c("T2M_MAX","T2M_MIN","PRECTOT","RH2M","ALLSKY_SFC_SW_DWN")
start_year <- "2019"; end_year <- "2024"
base_url <- "https://power.larc.nasa.gov/api/temporal/monthly/point"

# Función para obtener datos climáticos
obtener_clima <- function(zona, lat, lon){
  cat("🔍 Consultando zona:", zona, "\n")
  params <- list(
    parameters = paste(variables, collapse=","), community="AG",
    longitude=lon, latitude=lat,
    start=start_year, end=end_year, format="JSON"
  )
  resp <- GET(url=base_url, query=params)
  if(status_code(resp)==200){
    contenido <- content(resp,"text",encoding="UTF-8")
    tryCatch({
      j <- fromJSON(contenido)
      param <- j$properties$parameter
      fechas <- names(param[[1]])
      df <- data.frame(Fecha=fechas,
                       Temp_Max=unlist(param$T2M_MAX),
                       Temp_Min=unlist(param$T2M_MIN),
                       Precipitacion=unlist(param$PRECTOT),
                       Humedad=unlist(param$RH2M),
                       RadiacionSolar=unlist(param$ALLSKY_SFC_SW_DWN),
                       Zona=zona, Latitud=lat, Longitud=lon)
      df <- df %>%
        filter(!grepl("13$", Fecha)) %>%
        mutate(Año=as.integer(substr(Fecha,1,4)),
               Mes=as.integer(substr(Fecha,5,6))) %>%
        select(Zona,Latitud,Longitud,Año,Mes,Temp_Max,Temp_Min,Precipitacion,Humedad,RadiacionSolar)
      return(df)
    }, error = function(e){
      cat("❌ Error al procesar JSON para zona:", zona, "\n")
      return(NULL)
    })
  } else {
    cat("❌ Error HTTP para zona:", zona, "\n")
    return(NULL)
  }
}

# Recolección de datos para todas las zonas
datos <- pmap_dfr(list(zonas_banano$Zona, zonas_banano$Latitud, zonas_banano$Longitud),
                  obtener_clima)

# Guardar CSV
write_csv(datos, "clima_20zonas_banano_CR_2019_2024.csv")
cat("📁 Archivo final con datos climáticos de 20 zonas creado ✅\n")

