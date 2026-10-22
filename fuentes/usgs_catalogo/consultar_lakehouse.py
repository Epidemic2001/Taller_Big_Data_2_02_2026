"""Consulta el Data Lakehouse del catálogo USGS: contenido, particiones, versiones y Time Travel.

    python consultar_lakehouse.py                   # gold, particiones, snapshots y Time Travel
    python consultar_lakehouse.py --snapshot <id>   # Time Travel contra un snapshot elegido
"""
from __future__ import annotations

import argparse

import pandas as pd

from lakehouse import GOLD_TABLA, SILVER_TABLA, catalogo

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)


def main(snapshot_id: int | None) -> None:
    cat = catalogo()
    silver, gold = cat.load_table(SILVER_TABLA), cat.load_table(GOLD_TABLA)

    print(f"\n=== {GOLD_TABLA}: indicadores por mes")
    print(gold.scan().to_pandas().sort_values("mes").drop(columns="calculado_utc").to_string(index=False))

    print(f"\n=== {SILVER_TABLA}: particiones (ocultas, por mes de tiempo_utc)")
    part = silver.inspect.partitions().to_pandas()
    meses_desde_1970 = part["partition"].map(lambda p: p["mes"])
    part["mes"] = meses_desde_1970.map(lambda m: f"{1970 + m // 12}-{m % 12 + 1:02d}")
    print(part.sort_values("mes")[["mes", "record_count", "file_count"]].to_string(index=False))

    print(f"\n=== {SILVER_TABLA}: historial de snapshots (cada carga es una versión)")
    snaps = silver.inspect.snapshots().to_pandas().sort_values("committed_at")
    for clave in ("mes", "origen", "added-records", "deleted-records", "total-records"):
        snaps[clave] = snaps["summary"].map(lambda s: dict(s).get(clave))
    print(snaps[["committed_at", "snapshot_id", "operation", "mes", "origen",
                 "added-records", "deleted-records", "total-records"]].to_string(index=False))

    elegido = snapshot_id or int(snaps["snapshot_id"].iloc[0])
    print(f"\n=== Time Travel: sismos por mes en el snapshot {elegido} vs. la versión actual")
    antes = silver.scan(snapshot_id=elegido, selected_fields=("tiempo_utc",)).to_pandas()
    ahora = silver.scan(selected_fields=("tiempo_utc",)).to_pandas()
    print(pd.DataFrame({"en_snapshot": antes["tiempo_utc"].dt.strftime("%Y-%m").value_counts(),
                        "actual": ahora["tiempo_utc"].dt.strftime("%Y-%m").value_counts()})
          .fillna(0).astype(int).sort_index().to_string())

    print("\n=== Filtro resuelto por Iceberg (lee solo los archivos que pueden cumplirlo): sismos M>=5")
    fuertes = silver.scan(row_filter="magnitud >= 5.0",
                          selected_fields=("tiempo_utc", "magnitud", "profundidad_km", "lugar")).to_pandas()
    print(fuertes.sort_values("tiempo_utc").to_string(index=False) if len(fuertes) else "(ninguno)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--snapshot", type=int, help="id de snapshot para el Time Travel (por defecto, el primero)")
    main(ap.parse_args().snapshot)
