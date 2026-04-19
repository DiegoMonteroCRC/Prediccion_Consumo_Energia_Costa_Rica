"""Fila canonica del historico de distribucion electrica."""

from dataclasses import dataclass
from typing import ClassVar

from src.datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgDistribucion(_BaseStgModel):
    """Mapea columnas limpias de distribucion a stg_distribucion."""
    id_mes: int | None = None
    mes: str | None = None
    anho: int | None = None
    empresa: str | None = None
    tipo_tarifa: str | None = None
    descripcion_tarifa: str | None = None
    bloque: str | None = None
    tarifa_promedio: float | None = None

    aliases: ClassVar[dict] = {
        "id_mes": "id_Mes",
        "mes": "mes",
        "anho": "anho",
        "empresa": "empresa",
        "tipo_tarifa": "tipoTarifa",
        "descripcion_tarifa": "descripcionTarifa",
        "bloque": "bloque",
        "tarifa_promedio": "tarifaPromedio",
    }
