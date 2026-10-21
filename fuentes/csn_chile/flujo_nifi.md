# Flujo de Apache NiFi para CSN Chile (miniSEED)

NiFi orquesta la ingesta de archivos masivos: programa las descargas, reintenta
si fallan y deja trazabilidad (*data provenance*) de cada archivo. La lectura del
miniSEED la hace ObsPy, porque ninguna herramienta genérica de Big Data entiende
ese formato.

## Grupo de procesos "csn-incremental" (cada 5 minutos)

| # | Procesador NiFi | Configuración | Qué hace |
|---|---|---|---|
| 1 | `GenerateFlowFile` | Run Schedule: `5 min` | Dispara el ciclo |
| 2 | `ExecuteStreamCommand` | Command Path: `python` · Arguments: `fuentes/csn_chile/extraer.py;--modo;incremental` · Working Dir: carpeta del proyecto | Descarga el tramo nuevo, guarda el miniSEED crudo y el Parquet |
| 3 | `RouteOnAttribute` | `${execution.status:equals(0)}` | Separa éxito y error |
| 4 | `RetryFlowFile` → vuelve a 2 | Maximum Retries: `3` | Reintenta si el servidor del CSN no respondió |
| 5 | `LogAttribute` / `PutEmail` | — | Registra o avisa los errores definitivos |

## Grupo de procesos "csn-masivo" (una vez, backfill)

Igual que el anterior, pero `GenerateFlowFile` se ejecuta una sola vez y
`ExecuteStreamCommand` usa:
`fuentes/csn_chile/extraer.py;--modo;masivo;--inicio;2026-01-01T00:00;--fin;2026-10-01T00:00`

## Variante "NiFi puro" para la descarga

Si se quiere que NiFi haga también la descarga HTTP (sin Python en ese paso):

1. `GenerateFlowFile` (cada 5 min) con atributos `sta`, `cha`, `t0`, `t1`
2. `InvokeHTTP` GET
   `https://owl.csn.uchile.cl/fdsn95/dataselect/1/query?net=C1&sta=${sta}&cha=${cha}&starttime=${t0}&endtime=${t1}`
3. `UpdateAttribute` filename = `C1.${sta}.${cha}.${t0}.mseed`
4. `PutFile` → `datos/raw/csn_chile/estacion=${sta}/fecha=${t0:substring(0,10)}/`
5. `ExecuteStreamCommand` con ObsPy para convertir ese archivo a Parquet
