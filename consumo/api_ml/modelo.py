"""Modelo de ML del consumo 4: intensidad EMS-98 esperada según magnitud, profundidad y distancia.

Entrenamiento (lee directo del Data Warehouse, sin copiar los datos):
  1. Toma de ClickHouse (vista sismologia.v_testimonios) cada testimonio ciudadano de LastQuake con
     la magnitud y la profundidad de su sismo, la distancia al epicentro y la intensidad corregida.
  2. Compara dos modelos de scikit-learn con validación cruzada agrupada por sismo (GroupKFold): los
     testimonios de un mismo sismo nunca quedan a la vez en entrenamiento y en prueba, así el error
     medido es el de un sismo que el modelo no ha visto.
       - ecuacion_atenuacion: la forma clásica de una ecuación de predicción de intensidad,
         I = c0 + c1·M + c2·ln(R) + c3·R, con R la distancia hipocentral (regresión lineal).
       - gradient_boosting: HistGradientBoostingRegressor con restricciones monótonas: la
         intensidad solo puede subir con la magnitud y bajar con la distancia.
  3. Elige el de menor error absoluto medio (MAE), lo reentrena con todos los datos y guarda una
     versión nueva en datos/modelos/intensidad_ems98/ (el .joblib y sus metadatos en .json).

Cada testimonio pesa lo mismo. Se probó dar el mismo peso a cada sismo (1/√ de sus testimonios) y
el error empeoró: sube el peso de los sismos chicos con pocos testimonios, que casi no muestran
atenuación porque solo reporta quien lo sintió (sesgo de los testimonios ciudadanos).

La incertidumbre sale de los residuos de la validación cruzada: intervalo del 80 % y probabilidad
de cada nivel de daño. Los árboles no extrapolan, así que para escenarios fuera del rango de los
datos de entrenamiento (por ejemplo un M8 si el mayor sismo visto es M6) se responde con la
ecuación de atenuación, que sí sigue la forma física, y se avisa en la respuesta.
"""
from __future__ import annotations

import io
import json
import os
import platform
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import requests
import sklearn
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

from escala_ems98 import describir

RAIZ = Path(__file__).resolve().parents[2]
# Desde Windows ClickHouse está en localhost:8123; dentro de Docker, el compose define clickhouse:8123
CLICKHOUSE = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123/")
USUARIO = os.environ.get("CLICKHOUSE_USER", "admin")
CLAVE = os.environ.get("CLICKHOUSE_PASSWORD", "sismologia")
CARPETA_MODELOS = Path(os.environ.get("CARPETA_MODELOS", RAIZ / "datos" / "modelos" / "intensidad_ems98"))

ENTRADAS = ["magnitud", "profundidad_km", "distancia_km"]
MIN_TESTIMONIOS, MIN_SISMOS = 50, 3
SATURACION_KM = 5.0     # cerca de la fuente la intensidad no crece sin límite
NIVELES_DANIO = [("sin daño", -np.inf, 6), ("leve", 6, 7), ("moderado", 7, 8), ("grave", 8, np.inf)]

# Testimonios aptos para el modelo. La vista hace LEFT JOIN: si falta el sismo en dim_evento,
# ClickHouse rellena la magnitud con 0 (no con NULL), por eso se filtra magnitud > 0.
APTOS = """magnitud > 0 AND distancia_epicentro_km IS NOT NULL AND distancia_epicentro_km <= 2000
           AND profundidad_km >= 0 AND intensidad_corregida BETWEEN 1 AND 12"""
# El alias no puede llamarse tiempo_utc: en ClickHouse taparía a la columna dentro del WHERE
SQL_TESTIMONIOS = f"""
    SELECT unid, toString(tiempo_utc) AS tiempo_txt, magnitud, profundidad_km,
           distancia_epicentro_km AS distancia_km, intensidad_corregida AS intensidad
    FROM v_testimonios WHERE {APTOS}"""
SQL_DESPUES_DE = " AND tiempo_utc > parseDateTime64BestEffort({corte:String}, 3, 'UTC')"
SQL_CONTEO = f"SELECT count() AS testimonios, uniqExact(unid) AS sismos FROM v_testimonios WHERE {APTOS}"


class SinWarehouse(RuntimeError):
    """ClickHouse no responde."""


class DatosInsuficientes(RuntimeError):
    """No hay testimonios suficientes para entrenar."""


# ------------------------------------------------------------------ datos

def consultar(sql: str, **parametros) -> pd.DataFrame:
    """Consulta por la interfaz HTTP de ClickHouse. Los parámetros van aparte ({nombre:Tipo} en el SQL)."""
    params = {"database": "sismologia", **{f"param_{k}": v for k, v in parametros.items()}}
    try:
        r = requests.post(CLICKHOUSE, params=params, auth=(USUARIO, CLAVE),
                          data=f"{sql} FORMAT CSVWithNames".encode(), timeout=120)
    except requests.RequestException as e:
        raise SinWarehouse(f"ClickHouse no responde en {CLICKHOUSE}: {e.__class__.__name__}") from None
    if r.status_code != 200:
        raise SinWarehouse(f"ClickHouse respondió {r.status_code}: {r.text.strip()[:300]}")
    return pd.read_csv(io.StringIO(r.text), na_values=["\\N"], dtype={"unid": str})


def cargar_testimonios(despues_de: str | None = None) -> pd.DataFrame:
    """Testimonios aptos para entrenar; con despues_de, solo los posteriores a esa fecha UTC."""
    if despues_de is None:
        datos = consultar(SQL_TESTIMONIOS)
    else:
        datos = consultar(SQL_TESTIMONIOS + SQL_DESPUES_DE, corte=despues_de)
    return datos.rename(columns={"tiempo_txt": "tiempo_utc"})


def contar_testimonios() -> dict:
    fila = consultar(SQL_CONTEO).iloc[0]
    return {"testimonios": int(fila["testimonios"]), "sismos": int(fila["sismos"])}


# ------------------------------------------------------------------ rasgos (funciones de módulo para que el .joblib las encuentre)

def distancia_hipocentral(X: np.ndarray) -> np.ndarray:
    profundidad, distancia = X[:, 1], X[:, 2]
    return np.sqrt(distancia ** 2 + profundidad ** 2 + SATURACION_KM ** 2)


def rasgos_atenuacion(X) -> np.ndarray:
    """[M, h, Repi] -> [M, -ln R, -R]: la forma de una ecuación de predicción de intensidad (IPE).

    Con los signos invertidos y coeficientes positivos (LinearRegression(positive=True)), la ecuación
    queda físicamente consistente: c1 ≥ 0 (sube con la magnitud), c2 ≤ 0 y c3 ≤ 0 (baja con la
    distancia, por expansión geométrica y por atenuación anelástica). Sin esa restricción, el sesgo de
    los testimonios (solo reporta quien lo sintió) puede dejar c3 > 0, y la intensidad volvería a
    subir a cientos de km del epicentro.
    """
    X = np.asarray(X, dtype=float)
    r = distancia_hipocentral(X)
    return np.column_stack([X[:, 0], -np.log(r), -r])


def rasgos_boosting(X) -> np.ndarray:
    """[M, h, Repi] -> [M, h, ln R]: el árbol ve la profundidad y la distancia hipocentral."""
    X = np.asarray(X, dtype=float)
    return np.column_stack([X[:, 0], X[:, 1], np.log(distancia_hipocentral(X))])


CANDIDATOS = {
    "ecuacion_atenuacion": "Ecuación de atenuación I = c0 + c1·M + c2·ln(R) + c3·R (regresión lineal)",
    "gradient_boosting": "HistGradientBoostingRegressor con restricciones monótonas "
                         "(sube con la magnitud, baja con la distancia)",
}


def crear(nombre: str) -> Pipeline:
    if nombre == "ecuacion_atenuacion":
        return Pipeline([("rasgos", FunctionTransformer(rasgos_atenuacion)),
                         ("regresor", LinearRegression(positive=True))])
    if nombre == "gradient_boosting":
        return Pipeline([("rasgos", FunctionTransformer(rasgos_boosting)),
                         # árboles chicos y hojas de 100 testimonios: con pocos sismos, más
                         # complejidad solo aprende el ruido de cada sismo (se probó en la validación)
                         ("regresor", HistGradientBoostingRegressor(
                             monotonic_cst=[1, 0, -1], learning_rate=0.05, max_iter=100, max_leaf_nodes=7,
                             min_samples_leaf=100, l2_regularization=1.0, early_stopping=False, random_state=0))])
    if nombre == "referencia_promedio":     # predice siempre el promedio: el piso que hay que superar
        return Pipeline([("regresor", DummyRegressor())])
    raise ValueError(nombre)


# ------------------------------------------------------------------ entrenamiento

def metricas(y: np.ndarray, p: np.ndarray) -> dict:
    error = y - p
    grados_y, grados_p = np.clip(np.floor(y), 1, 12), np.clip(np.floor(p), 1, 12)
    ss_tot = np.sum((y - y.mean()) ** 2)
    return {"mae": round(float(np.mean(np.abs(error))), 3),
            "rmse": round(float(np.sqrt(np.mean(error ** 2))), 3),
            "r2": round(float(1 - np.sum(error ** 2) / ss_tot), 3) if ss_tot > 0 else None,
            "acierto_grado": round(float(np.mean(grados_y == grados_p)), 3),
            "acierto_grado_mas_menos_1": round(float(np.mean(np.abs(grados_y - grados_p) <= 1)), 3)}


def termino(c: float, decimales: int) -> str:
    return f"{'+' if c >= 0 else '−'} {abs(c):.{decimales}f}"


def preparar(datos: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return datos[ENTRADAS].to_numpy(float), datos["intensidad"].to_numpy(float), datos["unid"].to_numpy()


def validar(nombre: str, X, y, grupos, k: int) -> np.ndarray:
    """Predicciones fuera de la muestra (out-of-fold): cada sismo lo predice un modelo que no lo vio."""
    oof = np.empty_like(y)
    for entrenamiento, prueba in GroupKFold(n_splits=k).split(X, y, grupos):
        m = crear(nombre).fit(X[entrenamiento], y[entrenamiento])
        oof[prueba] = np.clip(m.predict(X[prueba]), 1, 12)
    return oof


def entrenar(datos: pd.DataFrame | None = None, carpeta: Path = CARPETA_MODELOS) -> dict:
    """Entrena, compara y guarda una versión nueva del modelo. Devuelve sus metadatos."""
    if datos is None:
        datos = cargar_testimonios()
    datos = datos.dropna(subset=ENTRADAS + ["intensidad", "unid"])
    n_sismos = datos["unid"].nunique()
    if len(datos) < MIN_TESTIMONIOS or n_sismos < MIN_SISMOS:
        raise DatosInsuficientes(
            f"Hay {len(datos)} testimonios de {n_sismos} sismos en ClickHouse; se necesitan al menos "
            f"{MIN_TESTIMONIOS} testimonios de {MIN_SISMOS} sismos. Carguen testimonios con el sondeo de "
            "LastQuake y Spark (DEMO.md, sección 4) y vuelvan a entrenar.")
    X, y, grupos = preparar(datos)
    k = min(5, n_sismos)

    validacion, residuos = {}, {}
    for nombre in [*CANDIDATOS, "referencia_promedio"]:
        oof = validar(nombre, X, y, grupos, k)
        validacion[nombre] = metricas(y, oof)
        residuos[nombre] = y - oof
    elegido = min(CANDIDATOS, key=lambda n: validacion[n]["mae"])

    principal = crear(elegido).fit(X, y)
    respaldo = principal if elegido == "ecuacion_atenuacion" else crear("ecuacion_atenuacion").fit(X, y)
    lineal = respaldo.named_steps["regresor"]
    c0, c1 = float(lineal.intercept_), float(lineal.coef_[0])
    c2, c3 = -float(lineal.coef_[1]), -float(lineal.coef_[2])   # los rasgos van con signo invertido

    ahora = datetime.now(timezone.utc)
    version = f"v{ahora:%Y%m%dT%H%M%S}"
    sismos = datos.groupby("unid").agg(magnitud=("magnitud", "first"), profundidad_km=("profundidad_km", "first"))
    meta = {
        "version": version,
        "entrenado_utc": ahora.isoformat(timespec="seconds"),
        "algoritmo": elegido,
        "descripcion": CANDIDATOS[elegido],
        "fuente": "ClickHouse · sismologia.v_testimonios (testimonios EMSC LastQuake)",
        "corte_datos": {"testimonios": int(len(datos)), "sismos": int(n_sismos),
                        "desde_utc": str(datos["tiempo_utc"].min()), "hasta_utc": str(datos["tiempo_utc"].max())},
        "dominio": {"magnitud": [round(float(sismos["magnitud"].min()), 1), round(float(sismos["magnitud"].max()), 1)],
                    "profundidad_km": [round(float(sismos["profundidad_km"].min()), 1),
                                       round(float(sismos["profundidad_km"].max()), 1)],
                    "distancia_km": [round(float(datos["distancia_km"].min()), 1),
                                     round(float(datos["distancia_km"].max()), 1)]},
        "validacion": {"metodo": f"GroupKFold por sismo (k={k}): cada sismo lo predice un modelo que no lo vio",
                       "metricas": validacion},
        "ecuacion_atenuacion": {
            "formula": f"I = {c0:.3f} {termino(c1, 3)}·M {termino(c2, 3)}·ln(R) {termino(c3, 5)}·R",
            "R": f"distancia hipocentral en km: √(distancia² + profundidad² + {SATURACION_KM:g}²)",
            "coeficientes": {"c0": round(c0, 4), "c1": round(c1, 4), "c2": round(c2, 4), "c3": round(c3, 6)}},
        "librerias": {"python": platform.python_version(), "scikit-learn": sklearn.__version__},
    }
    carpeta.mkdir(parents=True, exist_ok=True)
    joblib.dump({"principal": principal, "respaldo": respaldo, "metadatos": meta,
                 "sismos": sorted(datos["unid"].unique()),
                 "residuos": np.percentile(residuos[elegido], np.arange(1, 100)),
                 "residuos_respaldo": np.percentile(residuos["ecuacion_atenuacion"], np.arange(1, 100))},
                carpeta / f"{version}.joblib")
    (carpeta / f"{version}.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return meta


def versiones(carpeta: Path = CARPETA_MODELOS) -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(carpeta.glob("v*.json"), reverse=True)]


# ------------------------------------------------------------------ predicción

class Predictor:
    """El modelo vigente: la última versión guardada."""

    def __init__(self, artefacto: dict):
        self.principal = artefacto["principal"]
        self.respaldo = artefacto["respaldo"]
        self.residuos = artefacto["residuos"]
        self.residuos_respaldo = artefacto["residuos_respaldo"]
        self.sismos = set(artefacto["sismos"])
        self.meta = artefacto["metadatos"]

    @classmethod
    def cargar(cls, carpeta: Path = CARPETA_MODELOS) -> "Predictor | None":
        archivos = sorted(carpeta.glob("v*.joblib"))
        return cls(joblib.load(archivos[-1])) if archivos else None

    @property
    def version(self) -> str:
        return self.meta["version"]

    def fuera_de_rango(self, X: np.ndarray) -> list[list[str]]:
        """Por cada escenario, qué entradas quedan fuera de lo que el modelo vio al entrenar."""
        d = self.meta["dominio"]
        limites = {"magnitud": (d["magnitud"][0] - 0.3, d["magnitud"][1] + 0.3, "M{:g}–{:g}"),
                   "profundidad_km": (d["profundidad_km"][0] - 10, d["profundidad_km"][1] + 10, "{:g}–{:g} km"),
                   "distancia_km": (0, d["distancia_km"][1] * 1.1, "{:g}–{:g} km")}
        avisos = [[] for _ in range(len(X))]
        for j, (nombre, (minimo, maximo, rango)) in enumerate(limites.items()):
            for i in np.flatnonzero((X[:, j] < minimo) | (X[:, j] > maximo)):
                avisos[i].append(f"{nombre} = {X[i, j]:g} está fuera del rango de entrenamiento "
                                 f"({rango.format(*d[nombre])})")
        return avisos

    def intensidades(self, X: np.ndarray) -> tuple[np.ndarray, np.ndarray, list[list[str]]]:
        """Intensidad predicha; fuera del rango de entrenamiento la da la ecuación de atenuación."""
        avisos = self.fuera_de_rango(X)
        fuera = np.array([bool(a) for a in avisos])
        p = np.clip(self.principal.predict(X), 1, 12)
        if fuera.any():
            p[fuera] = np.clip(self.respaldo.predict(X[fuera]), 1, 12)
        return p, fuera, avisos

    def predecir(self, magnitud, profundidad_km, distancia_km) -> list[dict]:
        X = np.column_stack([np.atleast_1d(magnitud), np.atleast_1d(profundidad_km),
                             np.atleast_1d(distancia_km)]).astype(float)
        p, fuera, avisos = self.intensidades(X)
        respuestas = []
        for i, intensidad in enumerate(p):
            # distribución de la intensidad real: la predicción más los errores vistos en la validación
            posibles = np.clip(intensidad + (self.residuos_respaldo if fuera[i] else self.residuos), 1, 12)
            respuestas.append({
                "intensidad_ems98": round(float(intensidad), 2),
                **describir(float(intensidad)),
                "intervalo_80": [round(float(posibles[9]), 2), round(float(posibles[89]), 2)],
                "probabilidad_danio": {nivel: round(float(np.mean((posibles >= desde) & (posibles < hasta))), 2)
                                       for nivel, desde, hasta in NIVELES_DANIO},
                "distancia_hipocentral_km": round(float(distancia_hipocentral(X[i:i + 1])[0]), 1),
                "modelo_usado": "ecuacion_atenuacion" if fuera[i] else self.meta["algoritmo"],
                "fuera_de_rango": bool(fuera[i]),
                "aviso": ("; ".join(avisos[i]) + ". Se usa la ecuación de atenuación, que extrapola según la "
                          "forma física; tómenlo como una estimación gruesa.") if fuera[i] else None,
            })
        return respuestas

    def evaluar(self, nuevos: pd.DataFrame, total_ahora: dict) -> dict:
        """Monitoreo: cómo le va al modelo con los testimonios que llegaron después de su corte, y si
        conviene reentrenarlo (más datos, más error que en la validación o sismos fuera de su rango)."""
        corte = self.meta["corte_datos"]
        mae_validacion = self.meta["validacion"]["metricas"][self.meta["algoritmo"]]["mae"]
        respuesta = {"version_modelo": self.version, "corte_utc": corte["hasta_utc"],
                     "entrenamiento": {"testimonios": corte["testimonios"], "sismos": corte["sismos"]},
                     "warehouse_ahora": total_ahora, "mae_validacion": mae_validacion}
        nuevos = nuevos.dropna(subset=ENTRADAS + ["intensidad", "unid"])
        if nuevos.empty:
            return {**respuesta, "testimonios_nuevos": 0, "sismos_nuevos": [], "metricas_nuevos": None,
                    "reentrenar": False,
                    "motivos": ["No hay testimonios posteriores al corte: el modelo está al día con el warehouse."]}

        X, y, _ = preparar(nuevos)
        p, _, _ = self.intensidades(X)
        m = metricas(y, p)
        sismos = nuevos.groupby("unid")["magnitud"].first()
        sismos_nuevos = sorted(set(sismos.index) - self.sismos)
        motivos = []
        proporcion = len(nuevos) / corte["testimonios"]
        if proporcion >= 0.25:
            motivos.append(f"Llegaron {len(nuevos)} testimonios nuevos, {proporcion:.0%} de los que se usaron "
                           "para entrenar.")
        if len(nuevos) >= 30 and m["mae"] > 1.25 * mae_validacion:
            motivos.append(f"Con los testimonios nuevos el error (MAE {m['mae']:.2f}) supera en más de 25 % al "
                           f"de la validación ({mae_validacion:.2f}): los datos cambiaron.")
        mmax = self.meta["dominio"]["magnitud"][1]
        mayores = sismos[sismos > mmax]
        if not mayores.empty:
            motivos.append(f"Llegaron sismos de magnitud mayor a la máxima vista al entrenar (M{mmax:g}): "
                           + ", ".join(f"{u} M{mag:g}" for u, mag in mayores.items())
                           + ". Reentrenar amplía el rango donde el modelo es confiable.")
        return {**respuesta, "testimonios_nuevos": int(len(nuevos)), "sismos_nuevos": sismos_nuevos,
                "metricas_nuevos": m, "reentrenar": bool(motivos),
                "motivos": motivos or [f"Llegaron {len(nuevos)} testimonios nuevos y el error se mantiene "
                                       f"(MAE {m['mae']:.2f} frente a {mae_validacion:.2f}): no hace falta reentrenar."]}
