"""Fila canonica del mapa de zonas de concesion por operador."""

from dataclasses import dataclass
from typing import ClassVar

from datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgZonasConcesion(_BaseStgModel):
    """Mapea la respuesta del endpoint de zonas de concesion al shape de staging."""
    id_objecto: int | None = None
    operador: str | None = None
    descripcion: str | None = None
    area: float | None = None
    coordenadas: str | None = None
    tipo_geometria: str | None = None

    aliases: ClassVar[dict] = {
        "id_objecto": "id_Objecto",
        "operador": "operador",
        "descripcion": "descripcion",
        "area": "area",
        "coordenadas": "coordenadas",
        "tipo_geometria": "tipo_geometria",
    }
