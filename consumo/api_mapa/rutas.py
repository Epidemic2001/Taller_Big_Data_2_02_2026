"""Rutas de emergencia sobre la red vial de Calama que evitan las zonas de mayor intensidad sísmica.

La red vial (tabla red_vial_calama) se convierte en un grafo: los nodos son los cruces y cada arista
es un tramo de calle. Cada tramo tiene un tiempo de viaje (longitud / velocidad de referencia) y, para
cada sismo, la intensidad MMI que recibió según el ShakeMap. Se calculan dos rutas con Dijkstra:

  directa  la más rápida, sin considerar el sismo (la que daría un navegador cualquiera).
  segura   cada tramo cuesta su tiempo multiplicado por un castigo que crece con la MMI
           (1 + (MMI - 4)², desde el grado V en adelante). Los tramos con MMI >= el umbral
           "evitar_mmi" se consideran bloqueados (posible daño en la vía, derrumbes o cables caídos)
           y cuestan 1000 veces más: la ruta solo los usa si no hay otra forma de llegar, por
           ejemplo para salir de la zona cuando el origen está dentro de ella, y entonces recorre
           el mínimo posible de ellos.

Las rutas se piensan para vehículos de emergencia: no consideran el sentido de tránsito.
"""
from __future__ import annotations

from collections import Counter

import networkx as nx
import numpy as np

from escala import GRADOS, describir, grado

MMI_SIN_CASTIGO = 4.0       # hasta el grado IV no hay daño potencial: el tramo cuesta solo su tiempo
CASTIGO_BLOQUEO = 1000.0    # un tramo bloqueado se usa solo si no queda otra
DISTANCIA_MAX_RED_M = 3000  # un punto más lejos que esto de la red vial no se puede rutear


class PuntoFueraDeRed(ValueError):
    pass


def castigo(mmi: float | None) -> float:
    """Cuántas veces más "cuesta" recorrer un tramo según su intensidad: V -> 2, VI -> 5, VII -> 10."""
    if mmi is None:
        return 1.0
    return 1.0 + max(0.0, mmi - MMI_SIN_CASTIGO) ** 2


class RedVial:
    """Grafo de la red vial cargado en memoria una vez; las intensidades de cada sismo se pasan aparte."""

    def __init__(self, filas, firma):
        self.firma = firma
        self.grafo = nx.Graph()
        self.tramos = {}
        xy = {}
        for tid, a, b, tipo, nombre, velocidad, longitud, coords, xa, ya, xb, yb in filas:
            tiempo = longitud / (velocidad / 3.6)
            self.tramos[tid] = {"a": a, "nombre": nombre, "tipo": tipo, "longitud_m": longitud,
                                "tiempo_s": tiempo, "coords": coords}
            previo = self.grafo.get_edge_data(a, b)
            if previo is None or previo["tiempo_s"] > tiempo:  # dos vías paralelas: queda la más rápida
                self.grafo.add_edge(a, b, id=tid, tiempo_s=tiempo)
            xy[a], xy[b] = (xa, ya), (xb, yb)
        # solo la componente conectada más grande: un trozo de camino aislado no lleva a ninguna parte
        principal = max(nx.connected_components(self.grafo), key=len) if self.grafo else set()
        self.nodos = np.array(sorted(principal), dtype=np.int64)
        self.xy = np.array([xy[n] for n in self.nodos], dtype=float).reshape(-1, 2)

    def nodo_cercano(self, x: float, y: float) -> tuple[int, float]:
        """Nodo de la red (componente principal) más cercano a un punto en UTM 19S."""
        if not len(self.nodos):
            raise PuntoFueraDeRed("La red vial está vacía: ejecuta consumo/api_mapa/cargar_calama.py")
        d = np.hypot(self.xy[:, 0] - x, self.xy[:, 1] - y)
        i = int(np.argmin(d))
        return int(self.nodos[i]), float(d[i])

    def anclar(self, x: float, y: float, que: str) -> int:
        nodo, distancia = self.nodo_cercano(x, y)
        if distancia > DISTANCIA_MAX_RED_M:
            raise PuntoFueraDeRed(f"El {que} está a {distancia / 1000:.1f} km de la red vial cargada "
                                  f"(máximo {DISTANCIA_MAX_RED_M / 1000:.0f} km): elige un punto en Calama "
                                  "o sus alrededores")
        return nodo

    @staticmethod
    def peso_seguro(mmi_tramos: dict[int, float], evitar_mmi: float | None):
        def peso(u, v, datos):
            mmi = mmi_tramos.get(datos["id"])
            bloqueado = evitar_mmi is not None and mmi is not None and mmi >= evitar_mmi
            return datos["tiempo_s"] * castigo(mmi) * (CASTIGO_BLOQUEO if bloqueado else 1.0)
        return peso

    def recorrido(self, camino: list[int], mmi_tramos: dict[int, float], evitar_mmi: float | None,
                  tipo: str) -> dict:
        """Une los tramos del camino en una línea y resume su exposición a la intensidad."""
        coords, distancia, tiempo, mmi_max = [], 0.0, 0.0, None
        km_por_grado, km_calle = Counter(), Counter()
        km_sobre_umbral = 0.0
        for u, v in zip(camino, camino[1:]):
            tid = self.grafo[u][v]["id"]
            t = self.tramos[tid]
            puntos = t["coords"] if t["a"] == u else t["coords"][::-1]
            coords.extend(puntos if not coords else puntos[1:])
            km = t["longitud_m"] / 1000
            distancia += km
            tiempo += t["tiempo_s"]
            mmi = mmi_tramos.get(tid)
            if mmi is not None:
                mmi_max = mmi if mmi_max is None else max(mmi_max, mmi)
                km_por_grado[grado(mmi)] += km
                if evitar_mmi is not None and mmi >= evitar_mmi:
                    km_sobre_umbral += km
            if t["nombre"]:
                km_calle[t["nombre"]] += km
        return {"type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    "ruta": tipo,
                    "distancia_km": round(distancia, 2),
                    "tiempo_min": round(tiempo / 60, 1),
                    "mmi_max": round(mmi_max, 2) if mmi_max is not None else None,
                    "intensidad_max": describir(mmi_max),
                    # tramos con MMI >= VI: desde ese grado hay daño potencial leve o más
                    "km_con_dano_potencial": round(sum(k for g, k in km_por_grado.items() if g >= 6), 2),
                    "km_sobre_umbral": round(km_sobre_umbral, 2),
                    "km_por_grado": {GRADOS[g][0]: round(k, 2) for g, k in sorted(km_por_grado.items())},
                    "calles_principales": [c for c, _ in km_calle.most_common(5)],
                }}

    def rutas(self, origen: int, destino: int, mmi_tramos: dict[int, float], evitar_mmi: float | None) -> dict:
        if origen == destino:
            raise PuntoFueraDeRed("El origen y el destino quedan en el mismo punto de la red vial")
        try:
            directa = nx.dijkstra_path(self.grafo, origen, destino, weight="tiempo_s")
        except nx.NetworkXNoPath:
            raise PuntoFueraDeRed("No hay camino por la red vial entre el origen y el destino") from None
        segura = self.recorrido(
            nx.dijkstra_path(self.grafo, origen, destino, weight=self.peso_seguro(mmi_tramos, evitar_mmi)),
            mmi_tramos, evitar_mmi, "segura")
        return {"segura": segura, "directa": self.recorrido(directa, mmi_tramos, evitar_mmi, "directa"),
                "aviso": aviso_bloqueo(segura, evitar_mmi)}

    def mejor_destino(self, origen: int, destinos: dict[int, list[dict]], mmi_tramos: dict[int, float],
                      evitar_mmi: float | None) -> tuple[list[int], dict, list[dict]]:
        """El destino al que se llega con menor costo seguro (por ejemplo, el hospital operativo más cercano)."""
        costos, caminos = nx.single_source_dijkstra(self.grafo, origen, weight=self.peso_seguro(mmi_tramos, evitar_mmi))
        alcanzables = []
        for nodo, lugares in destinos.items():
            if nodo in costos:
                tiempo = sum(self.tramos[self.grafo[u][v]["id"]]["tiempo_s"]
                             for u, v in zip(caminos[nodo], caminos[nodo][1:]))
                for lugar in lugares:
                    alcanzables.append({**lugar, "nodo": nodo, "costo": costos[nodo],
                                        "tiempo_min": round(tiempo / 60, 1)})
        if not alcanzables:
            raise PuntoFueraDeRed("Ningún destino está conectado por la red vial con el origen")
        alcanzables.sort(key=lambda x: x["costo"])
        mejor = alcanzables[0]
        return caminos[mejor["nodo"]], mejor, alcanzables

    def intensidad_por_grado(self, mmi_tramos: dict[int, float], evitar_mmi: float | None) -> dict:
        """Km de red vial por grado MMI y tramos bloqueados, y las líneas de los tramos con daño
        potencial (MMI >= V) agrupadas por (grado, bloqueado) para dibujarlas en el mapa."""
        km_por_grado: Counter = Counter()
        km_bloqueados = 0.0
        grupos: dict[tuple[int, bool], dict] = {}
        for tid, mmi in mmi_tramos.items():
            t = self.tramos.get(tid)
            if t is None:
                continue
            g, km = grado(mmi), t["longitud_m"] / 1000
            bloqueado = evitar_mmi is not None and mmi >= evitar_mmi
            km_por_grado[g] += km
            km_bloqueados += km if bloqueado else 0
            if g >= 5:
                grupo = grupos.setdefault((g, bloqueado), {"lineas": [], "km": 0.0})
                grupo["lineas"].append(t["coords"])
                grupo["km"] += km
        return {"km_total": sum(t["longitud_m"] for t in self.tramos.values()) / 1000,
                "km_por_grado": dict(sorted(km_por_grado.items())), "km_bloqueados": km_bloqueados,
                "grupos": dict(sorted(grupos.items()))}


def aviso_bloqueo(ruta: dict, evitar_mmi: float | None) -> str | None:
    km = ruta["properties"]["km_sobre_umbral"]
    if not km:
        return None
    return (f"No hay forma de llegar sin pasar por tramos con MMI ≥ {evitar_mmi} (el origen o el destino "
            f"está dentro de esa zona): la ruta segura recorre el mínimo posible de ellos, {km} km.")
