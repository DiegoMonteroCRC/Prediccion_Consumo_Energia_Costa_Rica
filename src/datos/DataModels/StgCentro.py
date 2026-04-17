"""Fila canonica del catalogo de centrales electricas."""

from dataclasses import dataclass
from typing import ClassVar

from src.datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgCentro(_BaseStgModel):
    """Mapea la informacion geografica de centrales a stg_centro."""
    id_objecto: int | None = None
    operador: str | None = None
    central_electrica: str | None = None
    fuente: str | None = None
    provincia: str | None = None
    canton: str | None = None
    distrito: str | None = None
    codigo_dta: int | None = None
    coordenada_x: float | None = None
    coordenada_y: float | None = None

    aliases: ClassVar[dict] = {
        "id_objecto": "id_Objecto",
        "operador": "operador",
        "central_electrica": "centralElectrica",
        "fuente": "fuente",
        "provincia": "provincia",
        "canton": "canton",
        "distrito": "distrito",
        "codigo_dta": "codigoDTA",
        "coordenada_x": "coordenadaX",
        "coordenada_y": "coordenadaY",
    }
