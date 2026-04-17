"""Fila canonica del historico de distribucion electrica."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from src.datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgDistribucion(_BaseStgModel):
    """Mapea columnas limpias de distribucion a stg_distribucion."""
    id_mes: int | None = None
    mes: str | None = None
    anho: int | None = None
    fecha: datetime | None = None
    empresa: str | None = None
    tipo_tarifa: str | None = None
    descripcion_tarifa: str | None = None
    bloque: str | None = None
    tarifa_promedio: float | None = None
    tarifa: float | None = None
    pliego: str | None = None
    estructura_costos: str | None = None
    numero_expediente: str | None = None
    numero_resolucion: str | None = None
    fecha_publicacion: datetime | None = None

    aliases: ClassVar[dict] = {
        "id_mes": "id_Mes",
        "mes": "mes",
        "anho": "anho",
        "fecha": "fecha",
        "empresa": "empresa",
        "tipo_tarifa": "tipoTarifa",
        "descripcion_tarifa": "descripcionTarifa",
        "bloque": "bloque",
        "tarifa_promedio": "tarifaPromedio",
        "tarifa": "tarifa",
        "pliego": "pliego",
        "estructura_costos": "estructuraCostos",
        "numero_expediente": "numeroExpediente",
        "numero_resolucion": "numeroResolucion",
        "fecha_publicacion": "fechaPublicacion",
    }
