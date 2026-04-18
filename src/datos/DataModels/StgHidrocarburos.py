"""Fila canonica del historico de hidrocarburos para staging."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from src.datos.DataModels._BaseStgModel import _BaseStgModel


@dataclass
class StgHidrocarburos(_BaseStgModel):
    """Mapea el dataset limpio de hidrocarburos al esquema stg_hidrocarburos."""
    numero_expediente: str | None = None
    numero_resolucion: str | None = None
    fecha_publicacion: datetime | None = None
    alcance_gaceta: str | None = None
    numero_gaceta: int | None = None
    producto: str | None = None
    tipo_cambio: float | None = None
    precio_referencia_internacional: float | None = None
    precio_colonizado: float | None = None
    otros_ingresos_prorrateados: float | None = None
    asig_cruz_pesc: float | None = None
    asig_cruz_minae: float | None = None
    sub_cruz_pesc: float | None = None
    sub_cruz_minae: float | None = None
    diferencial_precios: float | None = None
    impuesto_unico: float | None = None
    canon: float | None = None
    margen_operacion: float | None = None
    rend_tarif: float | None = None
    precio_plantel_sin_impuesto: float | None = None
    precio_con_impuesto: float | None = None
    margen_estaciones_terrestres: float | None = None
    margen_estaciones_aereas: float | None = None
    flete_estaciones: float | None = None
    margen_envasador: float | None = None
    margen_distribuidor: float | None = None
    margen_detallista: float | None = None
    rige: bool | None = None
    precio_consumidor_final_gas: float | None = None
    precio_final: float | None = None
    precio_final_sin_punto_fijo: float | None = None

    aliases: ClassVar[dict] = {
        "numero_expediente": "numeroExpediente",
        "numero_resolucion": "numeroResolucion",
        "fecha_publicacion": "fechaPublicacion",
        "alcance_gaceta": "alcanceGaceta",
        "numero_gaceta": "numeroGaceta",
        "producto": "producto",
        "tipo_cambio": "tipoCambio",
        "precio_referencia_internacional": "precioReferenciaInternacional",
        "precio_colonizado": "precioColonizado",
        "otros_ingresos_prorrateados": "otrosIngresosProrrateados",
        "asig_cruz_pesc": "asigCruzPesc",
        "asig_cruz_minae": "asigCruzMinae",
        "sub_cruz_pesc": "subCruzPesc",
        "sub_cruz_minae": "subCruzMinae",
        "diferencial_precios": "diferencialPrecios",
        "impuesto_unico": "impuestoUnico",
        "canon": "canon",
        "margen_operacion": "margenOperacion",
        "rend_tarif": "rendTarif",
        "precio_plantel_sin_impuesto": "precioPlantelSinImpuesto",
        "precio_con_impuesto": "precioConImpuesto",
        "margen_estaciones_terrestres": "margenEstacionesTerrestres",
        "margen_estaciones_aereas": "margenEstacionesAereas",
        "flete_estaciones": "fleteEstaciones",
        "margen_envasador": "margenEnvasador",
        "margen_distribuidor": "margenDistribuidos",
        "margen_detallista": "margenDetallista",
        "rige": "rige",
        "precio_consumidor_final_gas": "precioConsumidorFinalGas",
        "precio_final": "precioFinal",
        "precio_final_sin_punto_fijo": "precioFinalSinPuntoFijo",
    }
