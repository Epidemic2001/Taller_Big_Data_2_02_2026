"""Consulta el Data Lake del CSN en MinIO: zona raw, metadatos de cabecera y tensores curados.

    python consultar_datalake.py
"""
from __future__ import annotations

import io

import numpy as np
import obstore
import pandas as pd
import zarr

from datalake import FUENTE, bucket, particiones_raw

pd.set_option("display.width", 200)


def main() -> None:
    raw, curated = bucket("raw"), bucket("curated")

    print("\n=== Zona raw (escrita por NiFi): miniSEED por estación, canal y día")
    objetos = [o for lote in obstore.list(raw, prefix=f"{FUENTE}/") for o in lote]
    filas = [dict(parte.split("=", 1) for parte in o["path"].split("/")[1:-1]) | {"bytes": o["size"]}
             for o in objetos]
    if not filas:
        print("(vacía: ¿está corriendo el flujo de NiFi?)")
        return
    print(pd.DataFrame(filas).groupby(["estacion", "canal", "anio", "dia_juliano"])
          .agg(archivos=("bytes", "size"), kb=("bytes", lambda b: round(b.sum() / 1024)))
          .reset_index().to_string(index=False))

    print("\n=== Zona curated: metadatos de cabecera de los miniSEED (ObsPy)")
    cabeceras = pd.concat([pd.read_parquet(io.BytesIO(bytes(obstore.get(curated, o["path"]).bytes())))
                           for lote in obstore.list(curated, prefix=f"{FUENTE}/") for o in lote
                           if o["path"].endswith("cabeceras.parquet")])
    print(cabeceras.groupby(["estacion", "canal"]).agg(
        segmentos=("archivo", "size"), muestras_por_seg=("muestras_por_seg", "first"),
        codificacion=("codificacion", "first"), desde=("inicio_utc", "min"), hasta=("fin_utc", "max"))
        .reset_index().to_string(index=False))

    print("\n=== Zona curated: tensores Zarr para Deep Learning")
    tensores = []
    for prefijo in particiones_raw(raw):
        try:
            g = zarr.open_group(store=zarr.storage.ObjectStore(bucket("curated", f"{prefijo}/ventanas.zarr"),
                                                               read_only=True), mode="r")
        except FileNotFoundError:
            continue  # partición todavía sin curar
        inicios = pd.to_datetime(g["inicio_utc_ns"][:], utc=True)
        tensores.append({"estacion": g.attrs["estacion"], "canal": g.attrs["canal"],
                         "forma": str(g["trazas"].shape), "bloques": str(g["trazas"].chunks),
                         "unidad": g.attrs["unidad"], "hz": g.attrs["muestras_por_seg"],
                         "primera_ventana": inicios.min(), "ultima_ventana": inicios.max(), "grupo": g})
    print(pd.DataFrame(tensores).drop(columns="grupo").to_string(index=False))

    g = max(tensores, key=lambda t: t["grupo"]["trazas"].shape[0])["grupo"]
    lote = g["trazas"][:8]  # como un DataLoader: solo se descargan los bloques que contienen esas ventanas
    print(f"\n=== Lectura de un lote para entrenamiento: {g.attrs['estacion']}.{g.attrs['canal']}")
    print(f"lote {lote.shape} {lote.dtype} | amplitud máxima por ventana ({g.attrs['unidad']}): "
          f"{np.abs(lote).max(axis=1).round(9).tolist()}")


if __name__ == "__main__":
    main()
