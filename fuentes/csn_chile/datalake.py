"""Zona curada del Data Lake del CSN: de los miniSEED crudos que deja NiFi a tensores para Deep Learning.

Data Lake on-premise en MinIO (perfiles nifi y datalake del docker-compose), con dos buckets:
  raw     -> csn_chile/red=C1/estacion=.../canal=.../anio=AAAA/dia_juliano=DDD/*.mseed
             el miniSEED tal como lo entrega el CSN, escrito por NiFi (PutS3Object). Con versionado.
  curated -> csn_chile/red=C1/estacion=.../canal=.../anio=AAAA/dia_juliano=DDD/
               cabeceras.parquet  metadatos de cabecera de cada miniSEED crudo, leídos con ObsPy
               ventanas.zarr      tensor float32 [ventana, muestra] en unidades físicas (m/s o m/s²):
                                  ventanas de 60 s sin vacíos, alineadas al minuto UTC, listas para
                                  leerse por bloques desde PyTorch o TensorFlow

Cada partición (estación, canal y día) se reconstruye completa a partir de sus archivos crudos, así
que el resultado es idempotente; y solo se vuelve a curar si NiFi dejó archivos nuevos desde la última vez.

Uso:
    python datalake.py           # cura las particiones con miniSEED nuevos
    python datalake.py --todo    # reconstruye todas las particiones
"""
from __future__ import annotations

import argparse
import io
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import obstore
import pandas as pd
import zarr
from obspy import Stream, UTCDateTime, read
from obstore.store import S3Store

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import ahora_utc, log  # noqa: E402
from extraer import FUENTE, VENTANA_S, cliente, metadatos  # noqa: E402

# MinIO del data lake visto desde Windows (desde los contenedores es minio-datalake:9000)
MINIO = dict(endpoint="http://localhost:9002", access_key_id="admin", secret_access_key="sismologia2026",
             region="us-east-1", client_options={"allow_http": True})
CITA = "Universidad de Chile (2013). Red Sismologica Nacional. https://doi.org/10.7914/SN/C1"


def bucket(nombre: str, prefijo: str | None = None) -> S3Store:
    return S3Store(nombre, prefix=prefijo, **MINIO)


def particiones_raw(raw: S3Store) -> dict[str, list[str]]:
    """{prefijo de la partición: [archivos .mseed]} de todo lo que NiFi dejó en la zona raw."""
    particiones = defaultdict(list)
    for lote in obstore.list(raw, prefix=f"{FUENTE}/"):
        for obj in lote:
            if obj["path"].endswith(".mseed"):
                prefijo, archivo = obj["path"].rsplit("/", 1)
                particiones[prefijo].append(archivo)
    return {p: sorted(a) for p, a in sorted(particiones.items())}


def archivos_ya_curados(prefijo: str) -> list[str] | None:
    try:
        tensor = zarr.open_group(store=zarr.storage.ObjectStore(bucket("curated", f"{prefijo}/ventanas.zarr"),
                                                                read_only=True), mode="r")
    except FileNotFoundError:
        return None
    return tensor.attrs.get("archivos_raw")


def leer_raw(raw: S3Store, prefijo: str, archivos: list[str]) -> tuple[Stream, pd.DataFrame]:
    st, cabeceras = Stream(), []
    for archivo in archivos:
        contenido = bytes(obstore.get(raw, f"{prefijo}/{archivo}").bytes())
        tramo = read(io.BytesIO(contenido), format="MSEED")
        for tr in tramo:
            s = tr.stats
            cabeceras.append({
                "archivo": archivo, "bytes": len(contenido),
                "red": s.network, "estacion": s.station, "ubicacion": s.location, "canal": s.channel,
                "inicio_utc": s.starttime.datetime, "fin_utc": s.endtime.datetime,
                "muestras_por_seg": float(s.sampling_rate), "n_muestras": int(s.npts),
                "codificacion": s.mseed.encoding, "largo_registro": int(s.mseed.record_length),
                "calidad": s.mseed.dataquality,
            })
        st += tramo
    return st, pd.DataFrame(cabeceras)


def ventanas(st: Stream, inv) -> tuple[np.ndarray, np.ndarray, float, str]:
    """Une los tramos, pasa a unidades físicas y corta ventanas completas de VENTANA_S alineadas al minuto."""
    st.merge(method=1, fill_value=None)
    st = st.split()                       # cada tramo continuo por separado: una ventana nunca cruza un vacío
    st.detrend("demean")
    st.remove_sensitivity(inv)            # cuentas -> m/s (HH?) o m/s² (HN?)
    sr = float(st[0].stats.sampling_rate)
    n = int(round(VENTANA_S * sr))
    trazas, inicios = [], []
    for tr in st:
        if tr.stats.sampling_rate != sr:
            continue
        t0 = tr.stats.starttime
        primer_minuto = UTCDateTime(np.ceil(t0.timestamp / VENTANA_S) * VENTANA_S)
        for i in range(int(round((primer_minuto - t0) * sr)), tr.stats.npts - n + 1, n):
            trazas.append(tr.data[i:i + n].astype("float32"))
            inicios.append((t0 + i / sr).ns)
    unidad = "m/s2" if st[0].stats.channel[1] == "N" else "m/s"
    return (np.stack(trazas) if trazas else np.empty((0, n), "float32"),
            np.array(inicios, dtype="int64"), sr, unidad)


def curar_particion(raw: S3Store, prefijo: str, archivos: list[str]) -> int:
    p = dict(parte.split("=", 1) for parte in prefijo.split("/")[1:])
    st, cabeceras = leer_raw(raw, prefijo, archivos)
    datos, inicios, sr, unidad = ventanas(st, metadatos(cliente(), p["estacion"], p["canal"]))

    parquet = io.BytesIO()
    cabeceras.to_parquet(parquet, index=False)
    obstore.put(bucket("curated"), f"{prefijo}/cabeceras.parquet", parquet.getvalue())

    tensor = zarr.open_group(store=zarr.storage.ObjectStore(bucket("curated", f"{prefijo}/ventanas.zarr")),
                             mode="w")
    tensor.create_array("trazas", data=datos, chunks=(32, datos.shape[1]))  # 32 ventanas por bloque
    tensor.create_array("inicio_utc_ns", data=inicios, chunks=(max(len(inicios), 1),))
    tensor.attrs.update({
        "dimensiones": ["ventana", "muestra"], "red": p["red"], "estacion": p["estacion"],
        "canal": p["canal"], "muestras_por_seg": sr, "ventana_s": VENTANA_S, "unidad": unidad,
        "archivos_raw": archivos, "curado_utc": ahora_utc().isoformat(), "cita": CITA,
    })
    return len(datos)


def main(todo: bool) -> None:
    raw = bucket("raw")
    particiones = particiones_raw(raw)
    if not particiones:
        log(FUENTE, "La zona raw está vacía: ¿está corriendo el flujo de NiFi?")
        return
    curadas = sin_cambios = 0
    for prefijo, archivos in particiones.items():
        if not todo and archivos_ya_curados(prefijo) == archivos:
            sin_cambios += 1
            continue
        n = curar_particion(raw, prefijo, archivos)
        curadas += 1
        log(FUENTE, f"curated/{prefijo}: {len(archivos)} miniSEED -> {n} ventanas de {VENTANA_S}s")
    log(FUENTE, f"Fin: {curadas} particiones curadas, {sin_cambios} sin archivos nuevos")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--todo", action="store_true", help="reconstruir todas las particiones")
    main(ap.parse_args().todo)
