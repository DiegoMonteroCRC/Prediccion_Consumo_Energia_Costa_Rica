import folium


class MapaConcesiones:

    def __init__(self, gdf):
        self.gdf = gdf
        self.mapa = None

    def _color_por_operador(self, operador):
        operador_limpio = str(operador).strip().upper()

        colores = {
            "CNFL": "#1f77b4",
            "COOPESANTOS": "#2ca02c",
            "COOPELESCA": "#9467bd",
            "COOPEGUANACASTE": "#ff7f0e",
            "COOPEGUANACASTE R.L.": "#ff7f0e",
            "COOPEALFARORUIZ": "#d62728",
            "COOPEALFARO RUIZ": "#d62728",
            "ESPH": "#17becf",
            "JASEC": "#006400",
            "ICE": "#e41a1c"
        }

        return colores.get(operador_limpio, "#7f7f7f")

    def _descripcion_operador(self, operador):
        operador_limpio = str(operador).strip().upper()

        descripciones = {
            "CNFL": "Gran Área Metropolitana",
            "COOPESANTOS": "Zona de Los Santos",
            "COOPELESCA": "Zona Norte",
            "COOPEGUANACASTE": "Provincia de Guanacaste",
            "COOPEGUANACASTE R.L.": "Provincia de Guanacaste",
            "COOPEALFARORUIZ": "Zona de Zarcero y alrededores",
            "COOPEALFARO RUIZ": "Zona de Zarcero y alrededores",
            "ESPH": "Heredia",
            "JASEC": "Cartago",
            "ICE": "Cobertura nacional o zonas amplias del país"
        }

        return descripciones.get(operador_limpio, "Zona no definida")

    def crear_mapa_base(self):
        self.mapa = folium.Map(
            location=[9.75, -84.0],
            zoom_start=8,
            tiles="OpenStreetMap"
        )

        titulo_html = """
        <h3 align="center" style="font-size:20px"><b>
        Zonas de Concesión por Operador Eléctrico - Costa Rica
        </b></h3>
        """
        self.mapa.get_root().html.add_child(folium.Element(titulo_html))

        return self.mapa

    def agregar_poligonos(self):
        """Agrega los polígonos con tooltip y popup."""
        if self.mapa is None:
            raise ValueError("Primero debes crear el mapa base.")

        for _, fila in self.gdf.iterrows():
            color = self._color_por_operador(fila["operador"])
            descripcion_operador = self._descripcion_operador(fila["operador"])

            tooltip_html = f"""
            <b>Operador:</b> {fila['operador']}<br>
            <b>Zona:</b> {descripcion_operador}
            """

            popup_html = f"""
            <b>Operador:</b> {fila['operador']}<br>
            <b>Zona:</b> {descripcion_operador}<br>
            <b>Descripción del registro:</b> {fila['descripcion']}<br>
            <b>Área:</b> {round(fila['area'], 2)}
            """

            folium.GeoJson(
                data=fila["geometry"].__geo_interface__,
                style_function=lambda feature, color=color: {
                    "fillColor": color,
                    "color": "black",
                    "weight": 0.8,
                    "fillOpacity": 0.5
                },
                highlight_function=lambda feature: {
                    "weight": 2,
                    "color": "yellow"
                },
                tooltip=folium.Tooltip(tooltip_html),
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(self.mapa)

    def agregar_leyenda(self):
        """Agrega una leyenda fija al mapa."""
        leyenda_html = """
        <div style="
            position: fixed;
            bottom: 30px; left: 30px; width: 230px; height: auto;
            background-color: white;
            border: 2px solid grey;
            z-index: 9999;
            font-size: 14px;
            padding: 10px;
        ">
        <b>Operadores</b><br>
        <i style="background:#1f77b4;width:10px;height:10px;display:inline-block"></i> CNFL<br>
        <i style="background:#2ca02c;width:10px;height:10px;display:inline-block"></i> COOPESANTOS<br>
        <i style="background:#9467bd;width:10px;height:10px;display:inline-block"></i> COOPELESCA<br>
        <i style="background:#ff7f0e;width:10px;height:10px;display:inline-block"></i> COOPEGUANACASTE<br>
        <i style="background:#d62728;width:10px;height:10px;display:inline-block"></i> COOPEALFARORUIZ<br>
        <i style="background:#17becf;width:10px;height:10px;display:inline-block"></i> ESPH<br>
        <i style="background:#006400;width:10px;height:10px;display:inline-block"></i> JASEC<br>
        <i style="background:#e41a1c;width:10px;height:10px;display:inline-block"></i> ICE<br>
        </div>
        """
        self.mapa.get_root().html.add_child(folium.Element(leyenda_html))

    def guardar_mapa(self, ruta_salida):
        """Guarda el mapa en un archivo HTML."""
        if self.mapa is None:
            raise ValueError("No hay mapa creado para guardar.")

        self.mapa.save(ruta_salida)