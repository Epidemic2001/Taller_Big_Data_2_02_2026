"""Capitales regionales de Chile (y Calama), para simular qué intensidad recibiría cada una.

Coordenadas aproximadas del centro de cada ciudad (WGS84). Calama usa el mismo punto (la plaza) que
el consumo 3.
"""
from __future__ import annotations

import numpy as np

# (ciudad, región, latitud, longitud), de norte a sur
CIUDADES = [
    ("Arica", "Arica y Parinacota", -18.4783, -70.3126),
    ("Iquique", "Tarapacá", -20.2133, -70.1503),
    ("Calama", "Antofagasta", -22.4560, -68.9290),
    ("Antofagasta", "Antofagasta", -23.6509, -70.3975),
    ("Copiapó", "Atacama", -27.3668, -70.3314),
    ("La Serena", "Coquimbo", -29.9027, -71.2519),
    ("Valparaíso", "Valparaíso", -33.0472, -71.6127),
    ("Santiago", "Metropolitana", -33.4489, -70.6693),
    ("Rancagua", "O'Higgins", -34.1708, -70.7444),
    ("Talca", "Maule", -35.4264, -71.6554),
    ("Chillán", "Ñuble", -36.6063, -72.1034),
    ("Concepción", "Biobío", -36.8270, -73.0503),
    ("Temuco", "La Araucanía", -38.7359, -72.5904),
    ("Valdivia", "Los Ríos", -39.8142, -73.2459),
    ("Puerto Montt", "Los Lagos", -41.4693, -72.9424),
    ("Coyhaique", "Aysén", -45.5752, -72.0662),
    ("Punta Arenas", "Magallanes", -53.1638, -70.9171),
]


def distancia_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """Distancia sobre la superficie de la Tierra (haversine), igual que la que calcula Spark."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, np.asarray(lat2, float), np.asarray(lon2, float)))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(a))
