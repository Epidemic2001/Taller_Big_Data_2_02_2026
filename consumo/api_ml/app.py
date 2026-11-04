"""Consumo 4 - Endpoint de Machine Learning: intensidad EMS-98 esperada y nivel de daño.

Un modelo de scikit-learn entrenado con los testimonios ciudadanos de LastQuake que Spark carga en el
Data Warehouse (ClickHouse). Recibe magnitud, profundidad y distancia al epicentro y responde la
intensidad que se sentiría, su grado EMS-98, el nivel de daño esperado y su incertidumbre. Sirve para
simular escenarios en planes de emergencia comunales y en el diseño de obras.

Al arrancar carga la última versión guardada en datos/modelos/intensidad_ems98/; si no hay ninguna,
entrena una con lo que haya en ClickHouse. POST /api/modelo/reentrenar crea una versión nueva y
GET /api/modelo/evaluacion dice si conviene hacerlo.

    uvicorn app:app --port 8091          (desde consumo/api_ml)
    python consumo/api_ml/app.py         (lo mismo, desde la raíz del proyecto)

Simulador web: http://localhost:8091   ·   documentación interactiva de la API: http://localhost:8091/docs
"""
from __future__ import annotations

import os
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

CARPETA = Path(__file__).resolve().parent
sys.path.insert(0, str(CARPETA))
import modelo  # noqa: E402
from ciudades import CIUDADES, distancia_km  # noqa: E402
from escala_ems98 import leyenda  # noqa: E402

MAGNITUD = Annotated[float, Query(ge=1, le=10, description="Magnitud del sismo (Mw)", examples=[7.0])]
PROFUNDIDAD = Annotated[float, Query(ge=0, le=700, description="Profundidad del hipocentro en km", examples=[35])]

estado: dict = {"predictor": None, "error": None}
candado = threading.Lock()   # un solo entrenamiento a la vez


def log(mensaje: str) -> None:
    print(f"[api-ml] {mensaje}", flush=True)


def entrenar_y_cargar() -> dict:
    meta = modelo.entrenar()
    estado["predictor"], estado["error"] = modelo.Predictor.cargar(), None
    return meta


def iniciar() -> None:
    """Carga el modelo vigente; si no hay (o no se puede leer), entrena uno con lo que haya en ClickHouse."""
    try:
        estado["predictor"] = modelo.Predictor.cargar()
    except Exception as e:  # por ejemplo, un .joblib de una versión incompatible de scikit-learn
        log(f"No se pudo leer el último modelo ({e.__class__.__name__}: {e}); se entrena uno nuevo")
    if estado["predictor"] is not None:
        log(f"Modelo {estado['predictor'].version} cargado ({estado['predictor'].meta['algoritmo']})")
        return
    try:
        with candado:
            meta = entrenar_y_cargar()
        log(f"Modelo {meta['version']} entrenado con {meta['corte_datos']['testimonios']} testimonios")
    except (modelo.SinWarehouse, modelo.DatosInsuficientes) as e:
        estado["error"] = str(e)
        log(f"Sin modelo todavía: {e}")


@asynccontextmanager
async def ciclo_de_vida(_app: FastAPI):
    iniciar()
    yield


app = FastAPI(
    title="Intensidad sísmica esperada: endpoint de Machine Learning",
    description="Consumo del Data Warehouse de testimonios ciudadanos (ClickHouse). Con la magnitud, la "
                "profundidad y la distancia al epicentro, un modelo de scikit-learn responde la intensidad "
                "EMS-98 esperada, el nivel de daño y su incertidumbre. Testimonios © EMSC LastQuake (CC BY 4.0).",
    version="1.0", lifespan=ciclo_de_vida)


def vigente() -> modelo.Predictor:
    if estado["predictor"] is None:
        raise HTTPException(503, estado["error"] or "Todavía no hay un modelo entrenado: POST /api/modelo/reentrenar")
    return estado["predictor"]


# ------------------------------------------------------------------ predicción

class Escenario(BaseModel):
    nombre: str | None = Field(None, description="Etiqueta libre para reconocer el escenario en la respuesta")
    magnitud: float = Field(ge=1, le=10, description="Magnitud del sismo (Mw)")
    profundidad_km: float = Field(ge=0, le=700, description="Profundidad del hipocentro en km")
    distancia_km: float = Field(ge=0, le=3000, description="Distancia al epicentro en km")


class Lote(BaseModel):
    escenarios: list[Escenario] = Field(min_length=1, max_length=1000)

    model_config = {"json_schema_extra": {"examples": [{"escenarios": [
        {"nombre": "Calama, sismo intraplaca", "magnitud": 6.8, "profundidad_km": 110, "distancia_km": 40},
        {"nombre": "Antofagasta, sismo interplaca", "magnitud": 8.0, "profundidad_km": 30, "distancia_km": 60},
        {"nombre": "Santiago, sismo lejano", "magnitud": 7.0, "profundidad_km": 40, "distancia_km": 250}]}]}}


@app.get("/api/predecir", tags=["predicción"],
         summary="Intensidad EMS-98 esperada y nivel de daño de un escenario")
def predecir(magnitud: MAGNITUD, profundidad_km: PROFUNDIDAD,
             distancia_km: Annotated[float, Query(ge=0, le=3000, description="Distancia al epicentro en km",
                                                  examples=[60])]):
    p = vigente()
    [r] = p.predecir(magnitud, profundidad_km, distancia_km)
    return {"escenario": {"magnitud": magnitud, "profundidad_km": profundidad_km, "distancia_km": distancia_km},
            **r, "version_modelo": p.version}


@app.post("/api/predecir", tags=["predicción"],
          summary="Varios escenarios a la vez (hasta 1.000), por ejemplo para un plan de emergencia")
def predecir_lote(lote: Lote):
    p = vigente()
    e = lote.escenarios
    resultados = p.predecir([x.magnitud for x in e], [x.profundidad_km for x in e], [x.distancia_km for x in e])
    return {"version_modelo": p.version,
            "escenarios": [{"escenario": x.model_dump(), **r} for x, r in zip(e, resultados)]}


@app.get("/api/curva-atenuacion", tags=["predicción"],
         summary="Cómo baja la intensidad esperada con la distancia, para una magnitud y profundidad")
def curva_atenuacion(magnitud: MAGNITUD, profundidad_km: PROFUNDIDAD,
                     hasta_km: Annotated[float, Query(gt=0, le=3000)] = 400,
                     puntos: Annotated[int, Query(ge=2, le=500)] = 81):
    p = vigente()
    distancias = np.linspace(0, hasta_km, puntos)
    resultados = p.predecir(np.full(puntos, magnitud), np.full(puntos, profundidad_km), distancias)
    return {"magnitud": magnitud, "profundidad_km": profundidad_km, "version_modelo": p.version,
            "puntos": [{"distancia_km": round(float(d), 1), "intensidad_ems98": r["intensidad_ems98"],
                        "intervalo_80": r["intervalo_80"], "romano": r["romano"], "nivel_danio": r["nivel_danio"],
                        "fuera_de_rango": r["fuera_de_rango"]} for d, r in zip(distancias, resultados)]}


@app.get("/api/ciudades", tags=["predicción"],
         summary="Intensidad y daño esperados en cada capital regional y Calama, para un epicentro")
def ciudades(magnitud: MAGNITUD, profundidad_km: PROFUNDIDAD,
             latitud: Annotated[float, Query(ge=-90, le=90, description="Latitud del epicentro",
                                             examples=[-23.0])],
             longitud: Annotated[float, Query(ge=-180, le=180, description="Longitud del epicentro",
                                              examples=[-70.6])]):
    p = vigente()
    lats, lons = [c[2] for c in CIUDADES], [c[3] for c in CIUDADES]
    distancias = distancia_km(latitud, longitud, lats, lons)
    resultados = p.predecir(np.full(len(CIUDADES), magnitud), np.full(len(CIUDADES), profundidad_km), distancias)
    filas = [{"ciudad": c, "region": reg, "latitud": la, "longitud": lo, "distancia_km": round(float(d), 1), **r}
             for (c, reg, la, lo), d, r in zip(CIUDADES, distancias, resultados)]
    return {"epicentro": {"latitud": latitud, "longitud": longitud, "magnitud": magnitud,
                          "profundidad_km": profundidad_km},
            "version_modelo": p.version,
            "ciudades": sorted(filas, key=lambda f: -f["intensidad_ems98"])}


# ------------------------------------------------------------------ modelo (versiones, reentrenamiento, monitoreo)

@app.get("/api/modelo", tags=["modelo"], summary="Modelo vigente: algoritmo, corte de datos, rango y métricas")
def modelo_vigente():
    return vigente().meta


@app.get("/api/modelo/versiones", tags=["modelo"], summary="Todas las versiones entrenadas, de la más nueva a la más antigua")
def versiones():
    return [{"version": v["version"], "entrenado_utc": v["entrenado_utc"], "algoritmo": v["algoritmo"],
             "testimonios": v["corte_datos"]["testimonios"], "sismos": v["corte_datos"]["sismos"],
             "hasta_utc": v["corte_datos"]["hasta_utc"],
             "mae": v["validacion"]["metricas"][v["algoritmo"]]["mae"]} for v in modelo.versiones()]


@app.post("/api/modelo/reentrenar", tags=["modelo"],
          summary="Entrena una versión nueva con los testimonios que hoy tiene ClickHouse y la deja vigente")
def reentrenar():
    anterior = estado["predictor"].version if estado["predictor"] else None
    if not candado.acquire(blocking=False):
        raise HTTPException(409, "Ya hay un entrenamiento en curso")
    try:
        meta = entrenar_y_cargar()
    except modelo.SinWarehouse as e:
        raise HTTPException(503, str(e)) from None
    except modelo.DatosInsuficientes as e:
        raise HTTPException(422, str(e)) from None
    finally:
        candado.release()
    log(f"Reentrenado: {anterior} -> {meta['version']}")
    return {"version_anterior": anterior, "version_nueva": meta["version"], "modelo": meta}


@app.get("/api/modelo/evaluacion", tags=["modelo"],
         summary="¿Conviene reentrenar? Error del modelo con los testimonios llegados después de su corte")
def evaluacion():
    p = vigente()
    try:
        nuevos = modelo.cargar_testimonios(despues_de=p.meta["corte_datos"]["hasta_utc"])
        total = modelo.contar_testimonios()
    except modelo.SinWarehouse as e:
        raise HTTPException(503, str(e)) from None
    return p.evaluar(nuevos, total)


# ------------------------------------------------------------------ referencia

@app.get("/api/escala", tags=["referencia"], summary="Escala EMS-98: grados, colores y niveles de daño")
def escala():
    return leyenda()


@app.get("/api/salud", tags=["referencia"], summary="Estado de ClickHouse y del modelo cargado")
def salud():
    p = estado["predictor"]
    try:
        warehouse = {"estado": "ok", **modelo.contar_testimonios()}
    except modelo.SinWarehouse as e:
        warehouse = {"estado": "sin conexión", "detalle": str(e)}
    return {"clickhouse": warehouse, "modelo": p.version if p else None, "error": estado["error"]}


app.mount("/static", StaticFiles(directory=CARPETA / "static"), name="static")


@app.get("/", include_in_schema=False)
def simulador():
    return FileResponse(CARPETA / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PUERTO", 8091)))
