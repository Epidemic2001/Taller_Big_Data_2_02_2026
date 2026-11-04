"""Escala Macrosísmica Europea (EMS-98) y nivel de daño, con los mismos umbrales del Data Warehouse.

Los testimonios de LastQuake traen la intensidad corregida en la escala EMS-98 (Bossu et al., 2016).
El modelo predice esa intensidad como número continuo (por ejemplo 6.4); para mostrarla se usa el
grado entero (VI) y su nombre oficial. El nivel de daño usa exactamente los cortes con que Spark
llena `nivel_danio_estimado` en ClickHouse (fuentes/emsc_lastquake/warehouse.py):
VI levemente dañino -> leve, VII dañino -> moderado, VIII o más gravemente dañino -> grave.
"""
from __future__ import annotations

import math

# grado -> (romano, nombre EMS-98, color). Los colores siguen la leyenda del ShakeMap del consumo 3.
GRADOS = {
    1: ("I", "No sentido", "#FFFFFF"),
    2: ("II", "Apenas sentido", "#BFCCFF"),
    3: ("III", "Débil", "#A0E6FF"),
    4: ("IV", "Ampliamente observado", "#80FFFF"),
    5: ("V", "Fuerte", "#7AFF93"),
    6: ("VI", "Levemente dañino", "#FFFF00"),
    7: ("VII", "Dañino", "#FFC800"),
    8: ("VIII", "Gravemente dañino", "#FF9100"),
    9: ("IX", "Destructor", "#FF0000"),
    10: ("X", "Muy destructor", "#C80000"),
    11: ("XI", "Devastador", "#A00000"),
    12: ("XII", "Completamente devastador", "#800000"),
}

# nivel de daño -> (intensidad mínima, qué se espera en las construcciones)
NIVELES = {
    "sin daño": (1, "Sin daño en las construcciones. Puede caer algún objeto pequeño."),
    "leve": (6, "Daño leve: fisuras en estucos y caída de trozos pequeños en muchas construcciones."),
    "moderado": (7, "Daño moderado: grietas en muros y caída de chimeneas; las construcciones "
                    "más vulnerables pueden sufrir daño grave."),
    "grave": (8, "Daño grave: muchas construcciones vulnerables con daño estructural; "
                 "algunas pueden colapsar."),
}


def grado(intensidad: float) -> int:
    """Intensidad continua -> grado entero entre I y XII ([6, 7) es el grado VI, como en el warehouse)."""
    return min(12, max(1, math.floor(intensidad)))


def nivel_danio(intensidad: float) -> str:
    if intensidad >= 8:
        return "grave"
    if intensidad >= 7:
        return "moderado"
    if intensidad >= 6:
        return "leve"
    return "sin daño"


def describir(intensidad: float) -> dict:
    g = grado(intensidad)
    romano, nombre, color = GRADOS[g]
    nivel = nivel_danio(intensidad)
    return {"grado": g, "romano": romano, "descripcion": nombre, "color": color,
            "nivel_danio": nivel, "danio_esperado": NIVELES[nivel][1]}


def leyenda() -> dict:
    return {
        "grados": [{"grado": g, "romano": r, "descripcion": n, "color": c, "nivel_danio": nivel_danio(g)}
                   for g, (r, n, c) in GRADOS.items()],
        "niveles_danio": [{"nivel": nivel, "desde_intensidad": desde, "descripcion": texto}
                          for nivel, (desde, texto) in NIVELES.items()],
    }
