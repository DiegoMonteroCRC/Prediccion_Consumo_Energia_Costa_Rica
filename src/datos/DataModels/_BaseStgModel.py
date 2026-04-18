"""Base compartida para convertir filas pandas en parametros compatibles con SQL."""

from dataclasses import fields
from typing import get_args

import numpy as np
import pandas as pd


class _BaseStgModel:
    """Aplica aliases y normalizacion minima antes de serializar una fila."""
    aliases = {}

    @classmethod
    def _campo_acepta_bool(cls, campo):
        # Detecta bool y bool opcional para aplicar limpieza especial antes del insert.
        if campo.type is bool:
            return True
        return bool in get_args(campo.type)

    @classmethod
    def _normalizar_booleano(cls, valor):
        # Traduce variantes comunes de texto a True/False y vacios a NULL.
        if isinstance(valor, bool):
            return valor
        if isinstance(valor, str):
            valor_normalizado = valor.strip().lower()
            if valor_normalizado == "":
                return None
            if valor_normalizado in {"true", "t", "1", "si", "sí", "yes", "y"}:
                return True
            if valor_normalizado in {"false", "f", "0", "no", "n"}:
                return False
        return valor

    @classmethod
    def _normalizar_valor(cls, campo, valor):
        # Convierte valores pandas/numpy a tipos simples que psycopg2 entiende sin ambiguedad.
        if pd.isna(valor):
            return None
        if cls._campo_acepta_bool(campo):
            valor = cls._normalizar_booleano(valor)
        if isinstance(valor, pd.Timestamp):
            return valor.date()
        if isinstance(valor, np.generic):
            return valor.item()
        return valor

    @classmethod
    def from_row(cls, fila):
        """Construye el dataclass usando la fila original y sus nombres de origen."""
        valores = {}
        for campo in fields(cls):
            # aliases conecta el nombre del CSV/DataFrame con el nombre canonico del staging.
            origen = cls.aliases.get(campo.name, campo.name)
            valores[campo.name] = cls._normalizar_valor(campo, fila.get(origen))
        return cls(**valores)

    def to_params(self):
        """Entrega los valores en el mismo orden declarado por el dataclass."""
        return tuple(getattr(self, campo.name) for campo in fields(self))
