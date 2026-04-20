"""Fila canonica de clima NASA por central y mes para staging."""

from dataclasses import dataclass
from typing import ClassVar

from datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgClimaNasa(_BaseStgModel):
    """Mapea clima mensual por central al naming SQL de stg_clima_nasa."""

    id_objecto: int | None = None
    central_electrica: str | None = None
    operador: str | None = None
    empresa: str | None = None
    coordenada_x: float | None = None
    coordenada_y: float | None = None
    ano: int | None = None
    mes: int | None = None
    t2m: float | None = None
    ws10m: float | None = None
    cloud_amt: float | None = None
    rh2m: float | None = None
    t2m_max: float | None = None
    t2m_min: float | None = None
    cloud_od: float | None = None
    gwetroot: float | None = None
    ts: float | None = None
    prectotcorr: float | None = None
    allsky_sfc_sw_dwn: float | None = None
    ps: float | None = None
    t2mwet: float | None = None
    allsky_sfc_sw_diff: float | None = None
    allsky_sfc_lw_dwn: float | None = None

    aliases: ClassVar[dict] = {
        "id_objecto": "id_Objecto",
        "central_electrica": "centralElectrica",
        "operador": "operador",
        "empresa": "Empresa",
        "coordenada_x": "coordenadaX",
        "coordenada_y": "coordenadaY",
        "ano": "Año",
        "mes": "Mes",
        "t2m": "T2M",
        "ws10m": "WS10M",
        "cloud_amt": "CLOUD_AMT",
        "rh2m": "RH2M",
        "t2m_max": "T2M_MAX",
        "t2m_min": "T2M_MIN",
        "cloud_od": "CLOUD_OD",
        "gwetroot": "GWETROOT",
        "ts": "TS",
        "prectotcorr": "PRECTOTCORR",
        "allsky_sfc_sw_dwn": "ALLSKY_SFC_SW_DWN",
        "ps": "PS",
        "t2mwet": "T2MWET",
        "allsky_sfc_sw_diff": "ALLSKY_SFC_SW_DIFF",
        "allsky_sfc_lw_dwn": "ALLSKY_SFC_LW_DWN",
    }
