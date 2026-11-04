"""Entrena una versión nueva del modelo de intensidad EMS-98 con los testimonios de ClickHouse.

La API también lo hace sola al arrancar (si no hay modelo) y con POST /api/modelo/reentrenar; este
script sirve para entrenar desde la terminal y ver la comparación de los modelos.

    python consumo/api_ml/entrenar.py              # con ClickHouse levantado (perfil warehouse, spark o ml)
    python consumo/api_ml/entrenar.py --versiones  # lista las versiones guardadas
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import modelo  # noqa: E402


def imprimir(meta: dict) -> None:
    corte = meta["corte_datos"]
    print(f"Versión {meta['version']}  ·  {corte['testimonios']} testimonios de {corte['sismos']} sismos "
          f"({corte['desde_utc'][:10]} a {corte['hasta_utc'][:10]})")
    print(f"Validación: {meta['validacion']['metodo']}\n")
    print(f"{'modelo':<22}{'MAE':>7}{'RMSE':>7}{'R²':>7}{'grado':>8}{'±1 grado':>10}")
    for nombre, m in meta["validacion"]["metricas"].items():
        marca = "  <- elegido" if nombre == meta["algoritmo"] else ""
        r2 = "—" if m["r2"] is None else f"{m['r2']:.2f}"
        print(f"{nombre:<22}{m['mae']:>7.2f}{m['rmse']:>7.2f}{r2:>7}{m['acierto_grado']:>8.0%}"
              f"{m['acierto_grado_mas_menos_1']:>10.0%}{marca}")
    d = meta["dominio"]
    print(f"\nEcuación de atenuación: {meta['ecuacion_atenuacion']['formula']}")
    print(f"Rango de entrenamiento: M{d['magnitud'][0]}–{d['magnitud'][1]}, profundidad "
          f"{d['profundidad_km'][0]}–{d['profundidad_km'][1]} km, distancia {d['distancia_km'][0]}–"
          f"{d['distancia_km'][1]} km")
    print(f"Guardado en {modelo.CARPETA_MODELOS / (meta['version'] + '.joblib')}")


if __name__ == "__main__":
    if isinstance(sys.stdout, io.TextIOWrapper):  # la consola de Windows (cp1252) no tiene "√" ni "−"
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--versiones", action="store_true", help="lista las versiones guardadas y termina")
    a = ap.parse_args()
    if a.versiones:
        for v in modelo.versiones():
            print(f"{v['version']}  {v['algoritmo']:<20} MAE {v['validacion']['metricas'][v['algoritmo']]['mae']:.2f}"
                  f"  {v['corte_datos']['testimonios']} testimonios hasta {v['corte_datos']['hasta_utc'][:16]}")
        sys.exit(0)
    try:
        imprimir(modelo.entrenar())
    except (modelo.SinWarehouse, modelo.DatosInsuficientes) as e:
        sys.exit(f"No se pudo entrenar: {e}")
