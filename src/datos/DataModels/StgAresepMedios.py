"""Fila canonica de precios medios ARESEP para staging."""

from dataclasses import dataclass
from typing import ClassVar

from src.datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgAresepMedios(_BaseStgModel):
    """Mapea el dataset unificado de medios a stg_aresep_medios."""
    mes: int | None = None
    ano: int | None = None
    empresa: str | None = None
    tarifa: str | None = None
    abonados: float | None = None
    ventas: float | None = None
    ingreso_sin_cvg: float | None = None
    ingreso_con_cvg: float | None = None
    precio_medio_sin_cvg: float | None = None
    precio_medio_con_cvg: float | None = None
    trimestre: str | None = None
    sistema: str | None = None
    trimestral: str | None = None

    aliases: ClassVar[dict] = {
        "mes": "Mes",
        "ano": "Año",
        "empresa": "Empresa",
        "tarifa": "Tarifa",
        "abonados": "Abonados",
        "ventas": "Ventas",
        "ingreso_sin_cvg": "Ingreso sin CVG",
        "ingreso_con_cvg": "Ingreso con CVG",
        "precio_medio_sin_cvg": "Precio Medio sin CVG",
        "precio_medio_con_cvg": "Precio Medio con CVG",
        "trimestre": "Trimestre",
        "sistema": "Sistema",
        "trimestral": "Trimestral",
    }
