"""Herramientas de entrenamiento para modelos predictivos del proyecto."""

import joblib
import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score, train_test_split

from eda.ProcesadorEDA import ProcesadorEDA


class ModelosML(ProcesadorEDA):
    """Extiende ProcesadorEDA con estado y utilidades para entrenamiento ML."""

    def __init__(self):
        super().__init__()
        self.df_train = None
        self.df_validacion = None
        self.df_test = None
        self.modelo = None
        self.algoritmos = [
            ("RL", LinearRegression()),
            ("RF", RandomForestRegressor(random_state=42)),
        ]
        self.modelos = {}
        self.metricas = {}
        self.columnas_modelo = []
        self.columnas_entrada = []
        self.valores_relleno = {}
        self.mejor_modelo = None
        self.mejor_modelo_nombre = None
        self.target = None

    def split_datos(
            self,
            columna_homogenea=None,
            proporcion_train=0.70,
            proporcion_test=0.30,
            val=True,
            random_state=42,
            chain=True,
    ):
        """Divide self.df en train, validacion y test, opcionalmente estratificado."""
        if self.df is None:
            raise ValueError("No hay DataFrame cargado en self.df.")

        if self.df.empty:
            raise ValueError("self.df esta vacio. Carga datos antes de particionar.")

        if round(proporcion_train + proporcion_test, 10) != 1:
            raise ValueError("proporcion_train + proporcion_test debe sumar 1.")

        stratify = None
        if columna_homogenea is not None:
            if columna_homogenea not in self.df.columns:
                raise ValueError(f"La columna '{columna_homogenea}' no existe en self.df.")
            stratify = self.df[columna_homogenea]

        try:
            self.df_train, df_test_total = train_test_split(
                self.df,
                train_size=proporcion_train,
                test_size=proporcion_test,
                random_state=random_state,
                stratify=stratify,
            )
        except ValueError as exc:
            raise ValueError(
                "No se pudo hacer el split homogeneo. Revisa que la columna "
                f"'{columna_homogenea}' tenga suficientes filas por valor para "
                "las proporciones solicitadas."
            ) from exc

        if val:
            stratify_test = None
            if columna_homogenea is not None:
                stratify_test = df_test_total[columna_homogenea]

            try:
                self.df_validacion, self.df_test = train_test_split(
                    df_test_total,
                    train_size=0.50,
                    test_size=0.50,
                    random_state=random_state,
                    stratify=stratify_test,
                )
            except ValueError as exc:
                raise ValueError(
                    "No se pudo dividir validacion y test de forma homogenea. "
                    f"Revisa que la columna '{columna_homogenea}' tenga suficientes "
                    "filas en el bloque de prueba."
                ) from exc
        else:
            self.df_validacion = None
            self.df_test = df_test_total

        resultado = {
            "df_train": self.df_train,
            "df_validacion": self.df_validacion,
            "df_test": self.df_test,
            "modelo": self.modelo,
        }
        return self._chain_response(resultado, chain)

    def _algoritmos_seleccionados(self, modelos_usar=None):
        if modelos_usar is None:
            return self.algoritmos

        modelos_usar = set(modelos_usar)
        seleccionados = [(nombre, algoritmo) for nombre, algoritmo in self.algoritmos if nombre in modelos_usar]
        faltantes = modelos_usar - {nombre for nombre, _ in seleccionados}

        if faltantes:
            raise ValueError(f"No existen algoritmos con abreviacion: {sorted(faltantes)}")

        return seleccionados

    def _preparar_columnas_entrada(self, df, columnas_entrada):
        if columnas_entrada is None:
            columnas_entrada = [col for col in df.columns if col != self.target]

        faltantes = [col for col in columnas_entrada if col not in df.columns]
        if faltantes:
            raise ValueError(f"Columnas de entrada no existen en el DataFrame: {faltantes}")

        return columnas_entrada

    def _calcular_valores_relleno(self, df, columnas_entrada):
        valores_relleno = {}

        for columna in columnas_entrada:
            serie = df[columna].dropna()

            if serie.empty:
                valores_relleno[columna] = None
                continue

            if pd.api.types.is_numeric_dtype(serie):
                valores_relleno[columna] = serie.median()
                continue

            if pd.api.types.is_datetime64_any_dtype(serie):
                valores_relleno[columna] = serie.mode().iloc[0]
                continue

            modo = serie.mode()
            valores_relleno[columna] = modo.iloc[0] if not modo.empty else serie.iloc[0]

        return valores_relleno

    def _preparar_features(self, df, columnas_entrada, fit=False):
        columnas_preparadas = []

        for columna in columnas_entrada:
            serie = df[columna]

            if pd.api.types.is_datetime64_any_dtype(serie):
                columnas_preparadas.append(pd.to_datetime(serie, errors="coerce").astype("int64").rename(columna))
                continue

            serie_numerica = pd.to_numeric(serie, errors="coerce")
            valores_no_nulos = serie.notna().sum()

            if valores_no_nulos == 0 or serie_numerica.notna().sum() == valores_no_nulos:
                columnas_preparadas.append(serie_numerica.rename(columna))
                continue

            dummies = pd.get_dummies(serie.astype("string").fillna("SIN_DATO"), prefix=columna)
            columnas_preparadas.append(dummies)

        X = pd.concat(columnas_preparadas, axis=1).apply(pd.to_numeric, errors="coerce").fillna(0)

        if fit:
            self.columnas_modelo = X.columns.tolist()
            return X

        return X.reindex(columns=self.columnas_modelo, fill_value=0)

    def _preparar_xy(self, df, columnas_entrada, fit=False):
        if self.target not in df.columns:
            raise ValueError(f"La columna objetivo '{self.target}' no existe en el DataFrame.")

        y = pd.to_numeric(df[self.target], errors="coerce")
        mascara = y.notna()

        X = self._preparar_features(df.loc[mascara], columnas_entrada, fit=fit)
        y = y.loc[mascara]

        if X.empty or y.empty:
            raise ValueError("No hay datos validos para entrenar despues de preparar X/y.")

        return X, y

    @staticmethod
    def _metricas_modelo(modelo, X, y):
        predicciones = modelo.predict(X)
        mse = mean_squared_error(y, predicciones)
        return {
            "r2": r2_score(y, predicciones),
            "mse": mse,
            "rmse": float(np.sqrt(mse)),
        }

    def entrenar_modelos(
            self,
            columna_objetivo,
            columnas_entrada=None,
            modelos_usar=None,
            cv=5,
            scoring="r2",
            chain=True,
    ):
        """Entrena los algoritmos seleccionados usando las particiones de la instancia."""
        if self.df_train is None:
            raise ValueError("No hay df_train. Ejecuta split_datos antes de entrenar.")

        self.target = columna_objetivo
        columnas_entrada = self._preparar_columnas_entrada(self.df_train, columnas_entrada)
        self.columnas_entrada = list(columnas_entrada)
        self.valores_relleno = self._calcular_valores_relleno(self.df_train, self.columnas_entrada)
        X_train, y_train = self._preparar_xy(self.df_train, columnas_entrada, fit=True)

        datos_validacion = None
        if self.df_validacion is not None:
            datos_validacion = self._preparar_xy(self.df_validacion, columnas_entrada, fit=False)

        datos_test = None
        if self.df_test is not None:
            datos_test = self._preparar_xy(self.df_test, columnas_entrada, fit=False)

        self.modelos = {}
        self.metricas = {}

        for nombre, algoritmo in self._algoritmos_seleccionados(modelos_usar):
            modelo_entrenado = clone(algoritmo)
            modelo_entrenado.fit(X_train, y_train)
            self.modelos[nombre] = modelo_entrenado

            cv_real = min(cv, len(X_train))
            if cv_real >= 2:
                cv_scores = cross_val_score(modelo_entrenado, X_train, y_train, cv=cv_real, scoring=scoring)
            else:
                cv_scores = np.array([np.nan])

            metricas = {
                "cv_mean": float(np.nanmean(cv_scores)),
                "cv_std": float(np.nanstd(cv_scores)),
            }

            if datos_validacion is not None:
                X_val, y_val = datos_validacion
                metricas |= {f"{k}_validacion": v for k, v in self._metricas_modelo(modelo_entrenado, X_val, y_val).items()}

            if datos_test is not None:
                X_test, y_test = datos_test
                metricas |= {f"{k}_test": v for k, v in self._metricas_modelo(modelo_entrenado, X_test, y_test).items()}

            self.metricas[nombre] = metricas

        self.modelo = self.modelos
        resultado = {
            "modelos": self.modelos,
            "metricas": self.metricas,
            "columnas_modelo": self.columnas_modelo,
            "columnas_entrada": self.columnas_entrada,
            "valores_relleno": self.valores_relleno,
            "target": self.target,
        }
        return self._chain_response(resultado, chain)

    def benchmark(self, chain=False):
        """Retorna una tabla comparativa con las metricas de los modelos entrenados."""
        if not self.metricas:
            raise ValueError("No hay metricas disponibles. Ejecuta entrenar_modelos primero.")

        resultado = pd.DataFrame.from_dict(self.metricas, orient="index")
        resultado.index.name = "modelo"
        return self._chain_response(resultado, chain)

    def evaluar_validacion(self, modelo="RF", usar_test=False, chain=False):
        """Compara real vs prediccion en validacion o test y calcula porcentaje de acierto."""
        if modelo not in self.modelos:
            disponibles = list(self.modelos.keys())
            raise ValueError(f"El modelo '{modelo}' no existe. Modelos disponibles: {disponibles}")

        df_eval = self.df_test if usar_test else self.df_validacion
        nombre_particion = "test" if usar_test else "validacion"

        if df_eval is None:
            raise ValueError(f"No hay df_{nombre_particion}. Ejecuta split_datos con la particion requerida.")

        X_eval, y_eval = self._preparar_xy(df_eval, self.columnas_entrada, fit=False)
        predicciones = self.modelos[modelo].predict(X_eval)

        resultado = df_eval.loc[y_eval.index].reset_index(drop=True).copy()
        resultado["real"] = y_eval.reset_index(drop=True)
        resultado["prediccion"] = predicciones
        resultado["error"] = resultado["real"] - resultado["prediccion"]
        resultado["error_absoluto"] = resultado["error"].abs()
        resultado["error_porcentaje"] = np.where(
            resultado["real"] != 0,
            (resultado["error_absoluto"] / resultado["real"].abs()) * 100,
            np.nan,
        )
        resultado["acierto_porcentaje"] = (100 - resultado["error_porcentaje"]).clip(lower=0)
        resultado["modelo"] = modelo
        resultado["particion"] = nombre_particion

        return self._chain_response(resultado, chain)

    def comparar_modelos(self, chain=False):
        """Compara los modelos entrenados en validacion y test con porcentajes simples."""
        if not self.modelos:
            raise ValueError("No hay modelos entrenados. Ejecuta entrenar_modelos primero.")

        filas = []

        for nombre_modelo in self.modelos:
            fila = {"modelo": nombre_modelo}

            if self.df_validacion is not None:
                validacion = self.evaluar_validacion(modelo=nombre_modelo, usar_test=False, chain=False)
                fila["acierto_validacion_%"] = round(validacion["acierto_porcentaje"].mean(), 2)
                fila["error_validacion_%"] = round(validacion["error_porcentaje"].mean(), 2)

            if self.df_test is not None:
                test = self.evaluar_validacion(modelo=nombre_modelo, usar_test=True, chain=False)
                fila["acierto_prueba_%"] = round(test["acierto_porcentaje"].mean(), 2)
                fila["error_prueba_%"] = round(test["error_porcentaje"].mean(), 2)

            metricas_modelo = self.metricas.get(nombre_modelo, {})
            if "r2_validacion" in metricas_modelo:
                fila["r2_validacion"] = round(metricas_modelo["r2_validacion"], 4)
            if "r2_test" in metricas_modelo:
                fila["r2_prueba"] = round(metricas_modelo["r2_test"], 4)

            filas.append(fila)

        resultado = pd.DataFrame(filas)

        if "acierto_validacion_%" in resultado.columns:
            ordenar_por = "acierto_validacion_%"
        elif "acierto_prueba_%" in resultado.columns:
            ordenar_por = "acierto_prueba_%"
        else:
            ordenar_por = None

        if ordenar_por is not None:
            resultado = resultado.sort_values(ordenar_por, ascending=False).reset_index(drop=True)
            self.mejor_modelo_nombre = resultado.loc[0, "modelo"]
            self.mejor_modelo = self.modelos[self.mejor_modelo_nombre]
            self.modelo = {self.mejor_modelo_nombre: self.mejor_modelo}

        return self._chain_response(resultado, chain)

    def cargar_modelo(self, nombre, chain=True):
        """Carga un modelo .joblib guardado en src/modelos y restaura su estado."""
        nombre_archivo = nombre if str(nombre).endswith(".joblib") else f"{nombre}.joblib"
        ruta = self.BASE_DIR / "src" / "modelos" / nombre_archivo

        if not ruta.exists():
            raise FileNotFoundError(f"No existe el modelo guardado: {nombre_archivo}")

        payload = joblib.load(ruta)

        if isinstance(payload, dict) and "modelo" in payload:
            self.modelo = payload["modelo"]
            self.modelos = self.modelo if isinstance(self.modelo, dict) else {"MODELO": self.modelo}
            self.metricas = payload.get("metricas", {})
            self.columnas_modelo = list(payload.get("columnas_modelo", []))
            self.columnas_entrada = list(payload.get("columnas_entrada", []))
            self.valores_relleno = dict(payload.get("valores_relleno", {}))
            self.mejor_modelo = None
            self.mejor_modelo_nombre = None
            self.target = payload.get("target")
        else:
            self.modelo = payload
            self.modelos = {"MODELO": payload}
            self.metricas = {}
            self.columnas_modelo = []
            self.columnas_entrada = []
            self.valores_relleno = {}
            self.mejor_modelo = payload
            self.mejor_modelo_nombre = "MODELO"
            self.target = None
            payload = {"modelo": payload}

        return self._chain_response(payload, chain)

    def predecir(
            self,
            datos,
            modelo="RF",
            columnas_entrada=None,
            incluir_datos=True,
            incluir_rellenos=False,
            chain=False,
    ):
        """Predice nuevos valores usando uno de los modelos cargados o entrenados."""
        if not self.modelos:
            raise ValueError("No hay modelos cargados o entrenados. Ejecuta cargar_modelo o entrenar_modelos primero.")

        if not self.columnas_modelo:
            raise ValueError("No hay columnas_modelo para alinear la prediccion.")

        if modelo not in self.modelos:
            disponibles = list(self.modelos.keys())
            raise ValueError(f"El modelo '{modelo}' no existe. Modelos disponibles: {disponibles}")

        if isinstance(datos, pd.DataFrame):
            df_prediccion = datos.copy()
        elif isinstance(datos, dict):
            df_prediccion = pd.DataFrame([datos])
        else:
            df_prediccion = pd.DataFrame(datos)

        if df_prediccion.empty:
            raise ValueError("No hay datos para predecir.")

        datos_salida = df_prediccion.copy()

        if columnas_entrada is None:
            columnas_entrada = self.columnas_entrada or [
                col for col in df_prediccion.columns if col != self.target
            ]

        for columna in columnas_entrada:
            valor_relleno = self.valores_relleno.get(columna)
            if columna not in df_prediccion.columns:
                df_prediccion[columna] = valor_relleno
            elif valor_relleno is not None:
                df_prediccion[columna] = df_prediccion[columna].fillna(valor_relleno)

        X = self._preparar_features(df_prediccion, columnas_entrada, fit=False)
        predicciones = self.modelos[modelo].predict(X)

        resultado = pd.DataFrame({"prediccion": predicciones})
        if incluir_datos:
            if incluir_rellenos:
                datos_salida = df_prediccion[columnas_entrada].reset_index(drop=True)
            resultado = pd.concat([datos_salida.reset_index(drop=True), resultado], axis=1)

        return self._chain_response(resultado, chain)
