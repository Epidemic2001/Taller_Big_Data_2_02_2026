"""Escala de intensidad de Mercalli Modificada (MMI) y daño potencial, según la leyenda del USGS ShakeMap.

El ShakeMap entrega la MMI como número continuo (por ejemplo 6.4). Para mostrarla se usa el grado
entero (VI) y su descripción: cómo se percibe el movimiento y qué daño potencial causa. Los colores
son los de la leyenda oficial del ShakeMap, para que el mapa se lea igual que los del USGS.
"""
from __future__ import annotations

import math

# grado -> (romano, percepción, daño potencial, nivel de daño 0-6, color)
GRADOS = {
    1: ("I", "No sentido", "Ninguno", 0, "#FFFFFF"),
    2: ("II", "Débil", "Ninguno", 0, "#BFCCFF"),
    3: ("III", "Débil", "Ninguno", 0, "#A0E6FF"),
    4: ("IV", "Ligero", "Ninguno", 0, "#80FFFF"),
    5: ("V", "Moderado", "Muy leve", 1, "#7AFF93"),
    6: ("VI", "Fuerte", "Leve", 2, "#FFFF00"),
    7: ("VII", "Muy fuerte", "Moderado", 3, "#FFC800"),
    8: ("VIII", "Severo", "Moderado a fuerte", 4, "#FF9100"),
    9: ("IX", "Violento", "Fuerte", 5, "#FF0000"),
    10: ("X+", "Extremo", "Muy fuerte", 6, "#C80000"),
}
SIN_DATO = {"grado": None, "romano": "—", "percepcion": "Sin dato", "dano": "Sin dato",
            "nivel_dano": None, "color": "#9E9E9E"}


def grado(mmi: float | None) -> int | None:
    """MMI continua -> grado entero entre 1 y 10 (igual que las isosistas: [6, 7) es el grado VI)."""
    if mmi is None or math.isnan(mmi):
        return None
    return min(10, max(1, math.floor(mmi)))


def describir(mmi: float | None) -> dict:
    g = grado(mmi)
    if g is None:
        return dict(SIN_DATO)
    romano, percepcion, dano, nivel, color = GRADOS[g]
    return {"grado": g, "romano": romano, "percepcion": percepcion, "dano": dano,
            "nivel_dano": nivel, "color": color}


def leyenda() -> list[dict]:
    return [{"grado": g, "romano": r, "percepcion": p, "dano": d, "nivel_dano": n, "color": c}
            for g, (r, p, d, n, c) in GRADOS.items()]
